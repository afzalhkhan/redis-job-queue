// src/queue.test.ts
import { initRedis, redis } from "./redisClient";
import { Queue } from "./queue";
import { Job } from "./types";
import { getQueueMetrics } from "./metrics";


describe("Queue", () => {
  const queueName = "testqueue";
  let queue: Queue;

  beforeAll(async () => {
    await initRedis();
    queue = new Queue(queueName);
    jest.spyOn(console, "warn").mockImplementation(() => {});
  });

  beforeEach(async () => {
    await redis.flushAll();
  });

  afterAll(async () => {
    (console.warn as jest.Mock).mockRestore();
    await redis.quit();
  });

  // ── enqueue ──────────────────────────────────────────────────────────────

  it("enqueues a job with correct defaults", async () => {
    const before = Date.now();
    const job    = await queue.enqueue({ foo: "bar" });

    expect(job.queueName).toBe(queueName);
    expect(job.priority).toBe("medium");
    expect(job.attemptsMade).toBe(0);
    expect(job.maxAttempts).toBe(3);
    expect(job.backoffMs).toBe(1000);
    expect(job.runAfter).toBeGreaterThanOrEqual(before);
  });

  it("respects delayMs when enqueuing", async () => {
    const delayMs = 5000;
    const before  = Date.now();
    const job     = await queue.enqueue({ foo: "bar" }, { delayMs });

    expect(job.runAfter).toBeGreaterThanOrEqual(before + delayMs);
    expect(job.runAfter - (before + delayMs)).toBeLessThanOrEqual(20_000);
  });


  it("promoteDelayedJobs moves due jobs into the waiting queue", async () => {
    const job        = await queue.enqueue({ foo: "delayed" }, { delayMs: 5_000, priority: "high" });
    const delayedKey = queue.delayedKeyName();
    const waitingKeys = queue.waitingKeys();

    expect(await redis.zCard(delayedKey)).toBe(1);
    expect(await redis.lLen(waitingKeys.high)).toBe(0);

    await redis.zAdd(delayedKey, [{ score: Date.now() - 1000, value: job.id }]);

    const moved = await queue.promoteDelayedJobs();
    expect(moved).toBe(1);

    expect(await redis.zCard(delayedKey)).toBe(0);
    expect(await redis.lLen(waitingKeys.high)).toBe(1);
  });


  it("recoverStuckJobs moves long-running active jobs back to waiting", async () => {
    const waitingKeys = queue.waitingKeys();
    const activeKey   = queue.activeKeyName();

    const job = await queue.enqueue({ foo: "stuck" }, { priority: "medium" });

    await redis.del(waitingKeys.medium);
    await redis.rPush(activeKey, job.id);

    job.status    = "active";
    job.startedAt = Date.now() - 31_000;
    await queue.saveJob(job);

    const recovered = await queue.recoverStuckJobs(30_000);
    expect(recovered).toBe(1);

    expect(await redis.lLen(activeKey)).toBe(0);
    expect(await redis.lLen(waitingKeys.medium)).toBe(1);

    const updated = await queue.getJob(job.id);
    expect(updated?.status).toBe("waiting");
    expect(updated?.startedAt).toBeUndefined();
  });


  it("markCompleted updates job status to completed and pushes to completed list", async () => {
    const job = await queue.enqueue({ foo: "done" });

    await queue.markCompleted(job.id);

    const updated = await queue.getJob(job.id);
    expect(updated?.status).toBe("completed");
    expect(updated?.startedAt).toBeUndefined();

    const completedList = await redis.lRange(`rq:queue:${queueName}:completed`, 0, -1);
    expect(completedList).toContain(job.id);
  });

  it("markFailed updates job status to failed and pushes to failed list", async () => {
    const job = await queue.enqueue({ foo: "broken" });

    await queue.markFailed(job.id);

    const updated = await queue.getJob(job.id);
    expect(updated?.status).toBe("failed");
    expect(updated?.startedAt).toBeUndefined();

    const failedList = await redis.lRange(`rq:queue:${queueName}:failed`, 0, -1);
    expect(failedList).toContain(job.id);
  });

  it("addHistoryEntry keeps only the latest 50 jobs", async () => {
    const historyKey = queue.historyKeyName();

    const base: Omit<Job, "id" | "queueName"> = {
      data: {},
      attemptsMade: 0,
      maxAttempts: 3,
      backoffMs: 1000,
      createdAt: 0,
      updatedAt: 0,
      runAfter: 0,
      status: "completed",
      priority: "medium",
    };

    // All 55 inserts run concurrently — each is an atomic LPUSH+LTRIM pipeline
    await Promise.all(
      Array.from({ length: 55 }, (_, i) =>
        queue.addHistoryEntry({ ...base, id: `job-${i}`, queueName })
      )
    );

    const raw = await redis.lRange(historyKey, 0, -1);
    expect(raw.length).toBe(50);
  });


  it("getQueueMetrics aggregates counts correctly", async () => {
    const j1 = await queue.enqueue({ t: 1 }, { priority: "high" });
    const j2 = await queue.enqueue({ t: 2 }, { priority: "medium" });
    const j3 = await queue.enqueue({ t: 3 }, { priority: "low" });

    await queue.markCompleted(j1.id);
    await queue.markFailed(j2.id);

    const metrics = await getQueueMetrics(queueName);

    expect(metrics.waiting).toBe(3);
    expect(metrics.waitingHigh).toBe(1);
    expect(metrics.waitingMedium).toBe(1);
    expect(metrics.waitingLow).toBe(1);
    expect(metrics.completed).toBe(1);
    expect(metrics.failed).toBe(1);
  });
});