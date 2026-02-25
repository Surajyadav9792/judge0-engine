// worker.js
// BullMQ-based worker that runs untrusted JavaScript inside Docker using dockerode.

const { PassThrough } = require('stream');
const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const Docker = require('dockerode');
const os = require('os');

const QUEUE_NAME = process.env.QUEUE_NAME || 'submissions';
const EXECUTION_TIMEOUT_MS = Number(process.env.EXECUTION_TIMEOUT_MS) || 5000;
const SUPPORTED_LANGUAGES = new Set(['node', 'javascript']);

const redisConnection = new IORedis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
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

  const langKey = language.toLowerCase();
  if (!SUPPORTED_LANGUAGES.has(langKey)) {
    return { error: createErrorResult(`Unsupported language: ${language}`) };
  }

  return { input: { sourceCode, stdin, expectedOutput } };
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

async function runInDocker({ sourceCode, stdin, expectedOutput }) {
  const image = 'node:18-alpine';
  const startTime = Date.now();

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
      Cmd: ['sh', '-c', 'echo "$USER_CODE" | base64 -d > /tmp/solution.js'],
      Env: [`USER_CODE=${sourceCodeBase64}`],
      AttachStdout: true,
      AttachStderr: true,
    });

    const writeResult = await executeAndCapture(writeExec);
    if (writeResult.exitCode !== 0) {
      throw new Error(writeResult.stderr || 'Failed to write source into container.');
    }

    const stdinBase64 = Buffer.from(String(stdin || ''), 'utf8').toString('base64');

    const runExec = await container.exec({
      Cmd: ['sh', '-c', 'echo "$STDIN_B64" | base64 -d | node /tmp/solution.js'],
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