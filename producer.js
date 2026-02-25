// producer.js
// Simple script to enqueue a test submission into the "submissions" queue.

const { Queue, QueueEvents } = require('bullmq');

const QUEUE_NAME = process.env.QUEUE_NAME || 'submissions';

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
};

async function main() {
  const queue = new Queue(QUEUE_NAME, { connection });
  const queueEvents = new QueueEvents(QUEUE_NAME, { connection });

  await queueEvents.waitUntilReady();

  // Example submission: reads from process.stdin
  const job = await queue.add('run-submission', {
    language: 'node',
    sourceCode: `
      let input = '';
      process.stdin.setEncoding('utf8');
      process.stdin.on('data', (chunk) => { input += chunk; });
      process.stdin.on('end', () => {
        console.log(input.toUpperCase().trim());
      });
    `,
    stdin: 'hello judge engine\n',
    expectedOutput: 'HELLO JUDGE ENGINE',
  });

  console.log(`[producer] Enqueued job with id: ${job.id}`);

  try {
    const result = await job.waitUntilFinished(queueEvents);
    console.log('[producer] Job result:', result);
  } catch (err) {
    console.error('[producer] Job failed:', err);
  } finally {
    await queueEvents.close();
    await queue.close();
  }
}

main().catch((err) => {
  console.error('[producer] Fatal error:', err);
  process.exit(1);
});

