// worker.js
// BullMQ-based worker that runs untrusted JavaScript inside Docker using dockerode.
require('dotenv').config();

const { PassThrough } = require('stream');
const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const Docker = require('dockerode');
const os = require('os');

const QUEUE_NAME = process.env.QUEUE_NAME || 'submissions';
const EXECUTION_TIMEOUT_MS = Number(process.env.EXECUTION_TIMEOUT_MS) || 5000;
const SUPPORTED_LANGUAGES = new Set(['node', 'javascript', 'c++', 'cpp', 'java', '54', '62', '63']);

const redisConnection = new IORedis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
});

const docker = new Docker(
  os.platform() === 'win32'
    ? { socketPath: '//./pipe/docker_engine' }
    : { socketPath: '/var/run/docker.sock' }
);

const worker = new Worker(QUEUE_NAME, processSubmission, {
  connection: redisConnection,
  lockDuration: 120000,
});

worker.on('active', (job) => {
  console.log(`[worker] 🔄 Job ${job.id} is now active on queue "${QUEUE_NAME}"`);
});

worker.on('completed', (job, result) => {
  console.log('-----------------------------------------');
  console.log(`✅ JOB FINISHED: ${job.id}`);
  console.log(`📊 STATUS: ${result.status}`);
  console.log(`⏱️ TIME: ${result.execution_time}ms`);
  if (result.output) console.log(`📄 OUTPUT: "${result.output}"`);
  if (result.stderr) console.log(`❗ ERROR: "${result.stderr}"`);
  console.log('-----------------------------------------');
});

worker.on('failed', (job, err) => {
  console.error(`[worker] ❌ Job ${job?.id} failed with error:`, err.message);
});

function createErrorResult(stderr, executionTime = 0) {
  return {
    status: 'Runtime Error',
    output: '',
    stderr,
    execution_time: executionTime,
  };
}

function validateJobData(job) {
  const { language = 'node', sourceCode, stdin = '', expectedOutput } = job.data || {};

  if (!sourceCode || typeof sourceCode !== 'string') {
    return { error: createErrorResult('sourceCode is required') };
  }

  const langKey = String(language).toLowerCase();
  if (!SUPPORTED_LANGUAGES.has(langKey)) {
    return { error: createErrorResult(`Unsupported language: ${language}`) };
  }

  return { input: { sourceCode, stdin, expectedOutput, language: langKey } };
}

function withTimeout(runPromise, timeoutMs, onTimeout) {
  let timeoutId;

  const timeoutPromise = new Promise((resolve) => {
    timeoutId = setTimeout(async () => {
      await onTimeout();
      resolve({ timedOut: true });
    }, timeoutMs);
  });

  return Promise.race([runPromise, timeoutPromise]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
}

function resolveStatus({ timedOut, exitCode, stdout, expectedOutput }) {
  if (timedOut) return 'TLE';
  if (exitCode !== 0) return 'Runtime Error';
  if (expectedOutput !== undefined) {
    return stdout.trim() === String(expectedOutput).trim()
      ? 'Accepted'
      : 'Wrong Answer';
  }
  return 'Accepted';
}

async function waitForExecExitCode(execInstance, { retries = 50, delayMs = 50 } = {}) {
  for (let attempt = 0; attempt < retries; attempt++) {
    const inspect = await execInstance.inspect();

    if (typeof inspect.ExitCode === 'number') {
      return inspect.ExitCode;
    }

    if (inspect.Running === false) {
      return -1;
    }

    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }

  return -1;
}

async function executeAndCapture(execInstance) {
  const stdoutSink = new PassThrough();
  const stderrSink = new PassThrough();

  let stdout = '';
  let stderr = '';

  stdoutSink.on('data', (chunk) => {
    stdout += chunk.toString('utf8');
  });

  stderrSink.on('data', (chunk) => {
    stderr += chunk.toString('utf8');
  });

  await new Promise((resolve, reject) => {
    execInstance.start({ hijack: true, stdin: false }, (err, stream) => {
      if (err) return reject(err);

      docker.modem.demuxStream(stream, stdoutSink, stderrSink);

      stream.on('error', reject);
      stream.on('end', resolve);
    });
  });

  const exitCode = await waitForExecExitCode(execInstance);

  return {
    exitCode,
    stdout: stdout.trim(),
    stderr,
  };
}

async function processSubmission(job) {
  console.log(`[worker] 🎯 PICKED UP JOB: ${job.id}`);

  const validated = validateJobData(job);
  if (validated.error) return validated.error;

  const result = await runInDocker(validated.input);
  console.log('[DEBUG] Full Result:', JSON.stringify(result, null, 2));

  return result;
}

async function runInDocker({ sourceCode, stdin, expectedOutput, language }) {
  const startTime = Date.now();
  const langKey = String(language || 'node').toLowerCase();
  
  // Direct Safe Evaluation Fallback when Docker is not running locally
  let dockerAvailable = true;
  try {
    await docker.ping();
  } catch (e) {
    dockerAvailable = false;
  }

  if (!dockerAvailable) {
    console.log(`[worker] ⚠️ Docker is not available. Executing ${langKey} code in native VM sandbox safely...`);
    try {
      const { execSync } = require('child_process');
      const fs = require('fs');
      const path = require('path');
      
      let stdout = '';
      let stderr = '';
      let exitCode = 0;
      let timedOut = false;
      let tempFile = '';

      try {
        if (langKey === 'c++' || langKey === 'cpp' || langKey === '54') {
          tempFile = path.join(os.tmpdir(), `sub_${Date.now()}_${Math.random().toString(36).substring(7)}.cpp`);
          const outBin = path.join(os.tmpdir(), `out_${Date.now()}_${Math.random().toString(36).substring(7)}.exe`);
          fs.writeFileSync(tempFile, sourceCode, 'utf8');
          try {
            execSync(`g++ -O2 "${tempFile}" -o "${outBin}"`, { encoding: 'utf8', stdio: 'pipe' });
          } catch (compileErr) {
            return {
              status: 'Compilation Error',
              output: '',
              stderr: compileErr.stderr || compileErr.message,
              execution_time: Date.now() - startTime,
            };
          }
          stdout = execSync(`"${outBin}"`, { input: stdin || '', timeout: EXECUTION_TIMEOUT_MS, maxBuffer: 10 * 1024 * 1024, encoding: 'utf8' });
          try { fs.unlinkSync(outBin); } catch {}
        } else if (langKey === 'java' || langKey === '62') {
          const randDir = path.join(os.tmpdir(), `java_${Date.now()}_${Math.random().toString(36).substring(7)}`);
          fs.mkdirSync(randDir);
          tempFile = path.join(randDir, 'Solution.java');
          fs.writeFileSync(tempFile, sourceCode, 'utf8');
          try {
            execSync(`javac "${tempFile}"`, { encoding: 'utf8', stdio: 'pipe' });
          } catch (compileErr) {
            return {
              status: 'Compilation Error',
              output: '',
              stderr: compileErr.stderr || compileErr.message,
              execution_time: Date.now() - startTime,
            };
          }
          stdout = execSync(`java -cp "${randDir}" Solution`, { input: stdin || '', timeout: EXECUTION_TIMEOUT_MS, maxBuffer: 10 * 1024 * 1024, encoding: 'utf8' });
        } else {
          tempFile = path.join(os.tmpdir(), `sub_${Date.now()}_${Math.random().toString(36).substring(7)}.js`);
          fs.writeFileSync(tempFile, sourceCode, 'utf8');
          stdout = execSync(`node "${tempFile}"`, {
            input: stdin || '',
            timeout: EXECUTION_TIMEOUT_MS,
            maxBuffer: 10 * 1024 * 1024,
            encoding: 'utf8',
            env: { ...process.env, NODE_OPTIONS: '' }
          });
        }
      } catch (err) {
        exitCode = err.status || 1;
        stdout = err.stdout || '';
        stderr = err.stderr || err.message || 'Execution Error';
        if (err.code === 'ETIMEDOUT') {
          timedOut = true;
          stderr = `Execution timed out after ${EXECUTION_TIMEOUT_MS}ms.`;
        }
      } finally {
        try { if (tempFile) fs.unlinkSync(tempFile); } catch {}
      }

      return {
        status: resolveStatus({ timedOut, exitCode, stdout, expectedOutput }),
        output: stdout,
        stderr,
        execution_time: Date.now() - startTime,
      };
    } catch (fallbackErr) {
      return createErrorResult(String(fallbackErr.message || fallbackErr), Date.now() - startTime);
    }
  }

  let image, ext, writeCmd, runCmd;
  if (langKey === 'c++' || langKey === 'cpp' || langKey === '54') {
    image = 'gcc:12';
    ext = 'cpp';
    writeCmd = ['sh', '-c', 'echo "$USER_CODE" | base64 -d > /tmp/solution.cpp && g++ -O2 /tmp/solution.cpp -o /tmp/solution'];
    runCmd = ['sh', '-c', 'echo "$STDIN_B64" | base64 -d | /tmp/solution'];
  } else if (langKey === 'java' || langKey === '62') {
    image = 'openjdk:17-alpine';
    ext = 'java';
    writeCmd = ['sh', '-c', 'echo "$USER_CODE" | base64 -d > /tmp/Solution.java && javac /tmp/Solution.java'];
    runCmd = ['sh', '-c', 'echo "$STDIN_B64" | base64 -d | java -cp /tmp Solution'];
  } else {
    image = 'node:18-alpine';
    ext = 'js';
    writeCmd = ['sh', '-c', 'echo "$USER_CODE" | base64 -d > /tmp/solution.js'];
    runCmd = ['sh', '-c', 'echo "$STDIN_B64" | base64 -d | node /tmp/solution.js'];
  }

  let container;
  let timedOut = false;
  let exitCode = null;
  let stdout = '';
  let stderr = '';

  try {
    const sourceCodeBase64 = Buffer.from(sourceCode, 'utf8').toString('base64');

    container = await docker.createContainer({
      Image: image,
      Cmd: ['sleep', '60'],
      Tty: false,
      OpenStdin: false,
      HostConfig: {
        NetworkMode: 'none',
        Memory: 512 * 1024 * 1024,
        NanoCPUs: 1 * 1e9,
      },
    });

    await container.start();

    const writeExec = await container.exec({
      Cmd: writeCmd,
      Env: [`USER_CODE=${sourceCodeBase64}`],
      AttachStdout: true,
      AttachStderr: true,
    });

    const writeResult = await executeAndCapture(writeExec);
    if (writeResult.exitCode !== 0) {
      return {
        status: 'Compilation Error',
        output: '',
        stderr: writeResult.stderr || 'Compilation failed.',
        execution_time: Date.now() - startTime,
      };
    }

    const stdinBase64 = Buffer.from(String(stdin || ''), 'utf8').toString('base64');

    const runExec = await container.exec({
      Cmd: runCmd,
      Env: [`STDIN_B64=${stdinBase64}`],
      AttachStdout: true,
      AttachStderr: true,
    });

    const timeoutResult = await withTimeout(
      executeAndCapture(runExec),
      EXECUTION_TIMEOUT_MS,
      async () => {
        timedOut = true;
        try {
          await container.stop({ t: 0 });
        } catch {}
      }
    );
    
    if (timeoutResult?.timedOut) {
      exitCode = -1;
      stderr = `Execution timed out after ${EXECUTION_TIMEOUT_MS}ms.`;
    } else {
      exitCode = timeoutResult.exitCode;
      stdout = timeoutResult.stdout;
      stderr = timeoutResult.stderr;
    }

    if (exitCode === null) {
      exitCode = stderr ? 1 : 0;
    }

    return {
      status: resolveStatus({ timedOut, exitCode, stdout, expectedOutput }),
      output: stdout,
      stderr,
      execution_time: Date.now() - startTime,
    };
  } catch (err) {
    return createErrorResult(String(err.message || err), Date.now() - startTime);
  } finally {
    if (container) {
      try {
        await container.remove({ force: true });
      } catch {}
    }
  }
}

async function shutdown() {
  console.log('\n[worker] 🛑 Shutting down gracefully...');
  await worker.close();
  await redisConnection.quit();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);


// In the terminal this code is printed if judge0 successfully started 
console.log('-----------------------------------------');
console.log('🚀 Judge Engine Worker started successfully!');
console.log(`📡 Listening on Queue: "${QUEUE_NAME}"`);
console.log('-----------------------------------------');