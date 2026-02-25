// src/example.ts
import { initRedis, redis } from "./redisClient";
import { Queue } from "./queue";
import { Worker } from "./worker";
import { Job } from "./types";

async function main() {
  await initRedis();

  const queue = new Queue("default");

  const worker = new Worker(
    queue,
    async (job: Job) => {
      console.log("[Example Handler] processing job", job.id, job.data);
      await new Promise((res) => setTimeout(res, 500));
    },
    { concurrency: 4, pollIntervalMs: 300, maxProcessingMs: 15_000 }
  );

  await worker.start();

  console.log("Dev example running. Enqueueing demo jobs every 2s...");

  const interval = setInterval(() => {
    queue.enqueue({ type: "email", userId: Math.floor(Math.random() * 1000) }, { priority: "medium" })
      .then((job) => {
        console.log("[Example] enqueued job", job.id);
      })
      .catch((err) => console.error("Enqueue error", err));
  }, 2000);


  const shutdown = async () => {
    console.log("\n[Example] Shutting down...");
    clearInterval(interval);
    await worker.stop();      
    await redis.quit();       
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("Fatal error in example:", err);
  process.exit(1);
});