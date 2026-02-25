// src/worker.ts
import { redis } from "./redisClient";
import { Job, JobHandler } from "./types";
import { Queue } from "./queue";

export interface WorkerOptions {
  concurrency?: number;
  pollIntervalMs?: number;
  maxProcessingMs?: number;
  maintenanceIntervalMs?: number; // how often to run recovery + promotion
}

export class Worker {
  private shouldStop       = false;
  private activeWorkers    = 0;
  private maintenanceTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly queue: Queue,
    private readonly handler: JobHandler,
    private readonly options: WorkerOptions = {}
  ) {}

  async start() {
    const concurrency           = this.options.concurrency           ?? 4;
    const pollIntervalMs        = this.options.pollIntervalMs        ?? 500;
    const maintenanceIntervalMs = this.options.maintenanceIntervalMs ?? 5_000;

    console.log(`[Worker] Starting for queue "${this.queue.name}" (concurrency=${concurrency})`);

    // Run maintenance (recovery + promotion) on a separate timer shared
    // across all poll loops — avoids hammering Redis on every tick.
    this.scheduleMaintenance(maintenanceIntervalMs);

    // Spin up N independent poll loops
    for (let i = 0; i < concurrency; i++) {
      this.pollLoop(pollIntervalMs);
    }
  }

  async stop() {
    console.log("[Worker] Stop requested");
    this.shouldStop = true;

    if (this.maintenanceTimer) {
      clearInterval(this.maintenanceTimer);
      this.maintenanceTimer = null;
    }
  }

  // ----- private -----

  /**
   * Runs recoverStuckJobs + promoteDelayedJobs once every
   * maintenanceIntervalMs from a single shared timer rather than
   * inside every poll loop iteration.
   */
  private scheduleMaintenance(intervalMs: number) {
    const maxProcessingMs = this.options.maxProcessingMs ?? 30_000;

    const run = async () => {
      if (this.shouldStop || !redis.isOpen) return;
      try {
        await this.queue.recoverStuckJobs(maxProcessingMs);
        await this.queue.promoteDelayedJobs();
      } catch (err) {
        console.error("[Worker] Maintenance error:", err);
      }
    };

    // Run once immediately on start, then on interval
    run();
    this.maintenanceTimer = setInterval(run, intervalMs);
  }

  private async pollLoop(pollIntervalMs: number) {
    while (!this.shouldStop) {
      try {
        if (!redis.isOpen) {
          console.warn("[Worker] Redis client is closed, exiting poll loop");
          break;
        }

        const jobId = await this.claimNextJob();
        if (!jobId) {
          await this.sleep(pollIntervalMs);
          continue;
        }

        this.activeWorkers++;
        await this.processJob(jobId);
        this.activeWorkers--;
      } catch (err: any) {
        const msg = String(err?.message ?? err);
        if (
          err?.name === "ClientClosedError" ||
          msg.includes("The client is closed")
        ) {
          console.warn("[Worker] Redis client closed, stopping worker loop");
          break;
        }

        console.error("[Worker] Poll loop error:", err);
        await this.sleep(pollIntervalMs);
      }
    }

    console.log("[Worker] Poll loop exited");
  }

  /**
   * Atomically moves a job id from waiting:* → active, respecting priority.
   * Uses LMOVE which is O(1) and safe under concurrent workers.
   */
  private async claimNextJob(): Promise<string | null> {
    const { high, medium, low } = this.queue.waitingKeys();
    const activeKey             = this.queue.activeKeyName();

    return (
      (await redis.lMove(high,   activeKey, "LEFT", "RIGHT")) ??
      (await redis.lMove(medium, activeKey, "LEFT", "RIGHT")) ??
      (await redis.lMove(low,    activeKey, "LEFT", "RIGHT")) ??
      null
    );
  }

  private async processJob(jobId: string) {
    const job = await this.queue.getJob(jobId);
    if (!job) {
      console.warn(`[Worker] Job not found: ${jobId}`);
      await this.queue.removeFromActive(jobId);
      return;
    }

    job.status    = "active";
    job.startedAt = Date.now();
    await this.queue.saveJob(job);

    try {
      await this.handler(job);

      // markCompleted now atomically updates the job hash + completed list
      await this.queue.markCompleted(job.id);
      await this.queue.removeFromActive(job.id);

      // Refresh job from Redis so addHistoryEntry sees the final state
      const completed = await this.queue.getJob(job.id);
      if (completed) await this.queue.addHistoryEntry(completed);

      console.log(`[Worker] Job ${job.id} completed`);
    } catch (err: any) {
      job.attemptsMade += 1;
      job.lastError     = String(err?.message ?? err);

      if (job.attemptsMade >= job.maxAttempts) {
        // Permanently failed — markFailed atomically updates hash + failed list
        await this.queue.saveJob(job); // persist lastError + attemptsMade first
        await this.queue.markFailed(job.id);
        await this.queue.removeFromActive(job.id);

        const failed = await this.queue.getJob(job.id);
        if (failed) await this.queue.addHistoryEntry(failed);

        console.error(
          `[Worker] Job ${job.id} failed permanently after ${job.attemptsMade} attempt(s)`
        );
      } else {
        // Retry with exponential-ready backoff via the delayed sorted set
        const nextRun  = Date.now() + job.backoffMs;
        job.runAfter   = nextRun;
        job.status     = "waiting";
        job.startedAt  = undefined;

        await this.queue.saveJob(job);
        await this.queue.removeFromActive(job.id);
        await redis.zAdd(this.queue.delayedKeyName(), [{ score: nextRun, value: job.id }]);

        console.warn(
          `[Worker] Job ${job.id} failed (attempt ${job.attemptsMade}/${job.maxAttempts}), ` +
          `retrying in ${job.backoffMs}ms`
        );
      }
    }
  }

  private sleep(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
  }
}