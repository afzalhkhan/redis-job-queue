// src/server.ts
import Fastify from "fastify";
import { initRedis } from "./redisClient";
import { Queue } from "./queue";
import { Worker } from "./worker";
import { Job } from "./types";
import { dashboard } from "./dashboard";
import { jobRoutes } from "./job";
import { metricsRoutes } from "./metricsCore";

const fastify = Fastify({
  logger: true,
});

const queues = new Map<
  string,
  { queue: Queue; worker: Worker; handler: (job: Job) => Promise<void> }
>();

export function getOrCreateQueue(name: string): Queue {
  const existing = queues.get(name);
  if (existing) return existing.queue;

  const queue = new Queue(name);

  const handler = async (job: Job) => {
    fastify.log.info(
      { jobId: job.id, data: job.data },
      `[Handler:${name}] Processing job`
    );

    // Example: random failure
    if (Math.random() < 0.2) {
      throw new Error("Random failure in REST worker");
    }

    // Simulate work
    await new Promise((res) => setTimeout(res, 300));
  };

  const worker = new Worker(queue, handler, {
    concurrency: 4,
    pollIntervalMs: 300,
    maxProcessingMs: 15_000,
  });

  worker.start(); // fire and forget

  queues.set(name, { queue, worker, handler });
  fastify.log.info(`[Server] Created queue + worker for "${name}"`);

  return queue;
}

// POST /enqueue
fastify.post("/enqueue", async (request, reply) => {
  try {
    const body = request.body as {
      queueName?: string;
      data?: any;
      delayMs?: number;
      maxAttempts?: number;
      backoffMs?: number;
      priority?: "high" | "medium" | "low";
    };

    const {
      queueName = "default",
      data,
      delayMs,
      maxAttempts,
      backoffMs,
      priority,
    } = body || {};

    if (data == null) {
      return reply
        .status(400)
        .send({ error: "data is required in request body" });
    }

    const queue = getOrCreateQueue(queueName);

    const job = await queue.enqueue(data, {
      delayMs,
      maxAttempts,
      backoffMs,
      priority,
    });

    return reply.send({ status: "enqueued", job });
  } catch (err: any) {
    fastify.log.error({ err }, "[/enqueue] Error");
    return reply.status(500).send({ error: String(err.message ?? err) });
  }
});



fastify.register(jobRoutes);

fastify.register(dashboard);

fastify.register(metricsRoutes);

async function start() {
  await initRedis();

  // ensure default queue + worker exist
  getOrCreateQueue("default");

  const port = Number(process.env.PORT) || 3000;
  const host = "0.0.0.0";

  try {
    await fastify.listen({ port, host });
    fastify.log.info(
      `[Server] Listening on http://localhost:${port} (Fastify)`
    );
    fastify.log.info(
      `[Server] Dashboard: http://localhost:${port}/dashboard?queueName=default`
    );
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

start();
