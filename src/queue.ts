// src/queue.ts
import { randomUUID } from "crypto";
import { redis } from "./redisClient";
import { EnqueueOptions, Job, Priority } from "./types";

const KEY_PREFIX = process.env.REDIS_NAMESPACE ?? "rq:";

export class Queue {
  constructor(public readonly name: string) {}

  // ----- key helpers -----

  private waitingKey(priority: Priority) {
    return `${KEY_PREFIX}queue:${this.name}:waiting:${priority}`;
  }

  private delayedKey() {
    return `${KEY_PREFIX}queue:${this.name}:delayed`;
  }

  private activeKey() {
    return `${KEY_PREFIX}queue:${this.name}:active`;
  }

  private jobKey(id: string) {
    return `${KEY_PREFIX}queue:${this.name}:job:${id}`;
  }

  private completedKey() {
    return `${KEY_PREFIX}queue:${this.name}:completed`;
  }

  private failedKey() {
    return `${KEY_PREFIX}queue:${this.name}:failed`;
  }

  private historyKey() {
    return `${KEY_PREFIX}queue:${this.name}:history`;
  }

  // ----- public key accessors (used by worker / server / metrics) -----

  activeKeyName()  { return this.activeKey();  }
  delayedKeyName() { return this.delayedKey(); }
  historyKeyName() { return this.historyKey(); }

  waitingKeys() {
    return {
      high:   this.waitingKey("high"),
      medium: this.waitingKey("medium"),
      low:    this.waitingKey("low"),
    };
  }

  // ----- core queue ops -----

  async enqueue(data: any, options: EnqueueOptions = {}): Promise<Job> {
    const id              = randomUUID();
    const now             = Date.now();
    const maxAttempts     = options.maxAttempts ?? 3;
    const backoffMs       = options.backoffMs   ?? 1000;
    const delayMs         = options.delayMs     ?? 0;
    const runAfter        = now + delayMs;
    const priority: Priority = options.priority ?? "medium";

    const job: Job = {
      id,
      queueName: this.name,
      data,
      attemptsMade: 0,
      maxAttempts,
      backoffMs,
      createdAt: now,
      updatedAt: now,
      runAfter,
      status: "waiting",
      priority,
    };

    await redis.set(this.jobKey(id), JSON.stringify(job));

    if (delayMs > 0) {
      await redis.zAdd(this.delayedKey(), [{ score: runAfter, value: id }]);
    } else {
      await redis.rPush(this.waitingKey(priority), id);
    }

    return job;
  }

  /**
   * Move due delayed jobs into the appropriate priority waiting queue.
   * Fetches all job payloads in parallel, then applies all Redis writes
   * in a single pipeline — O(n) round-trips reduced to 2.
   */
  async promoteDelayedJobs(): Promise<number> {
    const now        = Date.now();
    const delayedKey = this.delayedKey();

    const dueIds = await redis.zRangeByScore(delayedKey, 0, now);
    if (dueIds.length === 0) return 0;

    // Fetch all job payloads in parallel (1 round-trip for all)
    const raws = await Promise.all(dueIds.map((id) => redis.get(this.jobKey(id))));

    // Apply all promotions in one pipeline
    const pipeline = redis.multi();
    let moved = 0;

    for (let i = 0; i < dueIds.length; i++) {
      const id  = dueIds[i];
      const raw = raws[i];

      pipeline.zRem(delayedKey, id); // always remove from delayed

      if (!raw) continue; // stale id — just clean it up

      const job = JSON.parse(raw) as Job;
      pipeline.rPush(this.waitingKey(job.priority), job.id);
      moved++;
    }

    await pipeline.exec();
    return moved;
  }

  /**
   * Mark a job as completed: updates the job hash AND pushes to the
   * completed list atomically so status is never stale.
   */
  async markCompleted(jobId: string): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) return;

    job.status    = "completed";
    job.updatedAt = Date.now();
    job.startedAt = undefined;

    await redis
      .multi()
      .set(this.jobKey(jobId), JSON.stringify(job))
      .lPush(this.completedKey(), jobId)
      .exec();
  }

  /**
   * Mark a job as failed: updates the job hash AND pushes to the
   * failed list atomically so status is never stale.
   */
  async markFailed(jobId: string): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) return;

    job.status    = "failed";
    job.updatedAt = Date.now();
    job.startedAt = undefined;

    await redis
      .multi()
      .set(this.jobKey(jobId), JSON.stringify(job))
      .lPush(this.failedKey(), jobId)
      .exec();
  }

  async getJob(jobId: string): Promise<Job | null> {
    const raw = await redis.get(this.jobKey(jobId));
    if (!raw) return null;
    return JSON.parse(raw) as Job;
  }

  async saveJob(job: Job): Promise<void> {
    job.updatedAt = Date.now();
    await redis.set(this.jobKey(job.id), JSON.stringify(job));
  }

  async removeFromActive(jobId: string): Promise<void> {
    await redis.lRem(this.activeKey(), 0, jobId);
  }

  /**
   * Crash recovery: move jobs that have been active longer than
   * maxProcessingMs back to the waiting queue.
   * Fetches all job payloads in parallel, then applies all Redis writes
   * in a single pipeline — O(n) round-trips reduced to 2.
   */
  async recoverStuckJobs(maxProcessingMs: number): Promise<number> {
    const now       = Date.now();
    const activeKey = this.activeKey();

    const jobIds = await redis.lRange(activeKey, 0, -1);
    if (jobIds.length === 0) return 0;

    // Fetch all jobs in parallel (1 round-trip for all)
    const jobs = await Promise.all(jobIds.map((id) => this.getJob(id)));

    const pipeline = redis.multi();
    let recovered  = 0;

    for (let i = 0; i < jobIds.length; i++) {
      const id  = jobIds[i];
      const job = jobs[i];

      if (!job) {
        pipeline.lRem(activeKey, 0, id); // stale id — clean up
        continue;
      }

      if (
        job.status === "active" &&
        job.startedAt !== undefined &&
        now - job.startedAt > maxProcessingMs
      ) {
        job.status    = "waiting";
        job.startedAt = undefined;
        job.runAfter  = now;
        job.updatedAt = now;

        pipeline.set(this.jobKey(job.id), JSON.stringify(job));
        pipeline.lRem(activeKey, 0, job.id);
        pipeline.rPush(this.waitingKey(job.priority), job.id);
        recovered++;
      }
    }

    if (recovered > 0) {
      await pipeline.exec();
      console.warn(`[Queue:${this.name}] Recovered ${recovered} stuck active job(s)`);
    }

    return recovered;
  }

  /**
   * Store a compact history entry. Keeps only the newest 50 entries.
   * LPUSH + LTRIM are sent as one atomic pipeline — safe under concurrency.
   */
  async addHistoryEntry(job: Job): Promise<void> {
    const entry = {
      id:          job.id,
      status:      job.status,
      priority:    job.priority,
      attempts:    job.attemptsMade,
      maxAttempts: job.maxAttempts,
      createdAt:   job.createdAt,
      updatedAt:   job.updatedAt,
      lastError:   job.lastError ?? null,
    };

    await redis
      .multi()
      .lPush(this.historyKey(), JSON.stringify(entry))
      .lTrim(this.historyKey(), 0, 49)
      .exec();
  }
}