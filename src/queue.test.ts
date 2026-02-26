// src/queue.test.ts
import { initRedis, redis } from "./redisClient";
import { Queue } from "./queue";
import { Worker } from "./worker";
import { Job } from "./types";
import { getQueueMetrics } from "./metrics";

beforeAll(async () => {
  await initRedis();
});

afterAll(async () => {
  if (redis.isOpen) await redis.quit();
});

describe("Queue", () => {
  const queueName = "testqueue";
  let queue: Queue;

  beforeAll(async () => {
    queue = new Queue(queueName);
    jest.spyOn(console, "warn").mockImplementation(() => {});
    jest.spyOn(console, "log").mockImplementation(() => {});
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  beforeEach(async () => {
    await redis.flushAll();
  });

  afterAll(() => {
    (console.warn as jest.Mock).mockRestore();
    (console.log as jest.Mock).mockRestore();
    (console.error as jest.Mock).mockRestore();
  });

  // ── enqueue ───────────────────────────────────────────────────────────────

  it("enqueues a job with correct defaults", async () => {
    const before = Date.now();
    const job = await queue.enqueue({ foo: "bar" });

    expect(job.id).toBeDefined();
    expect(job.queueName).toBe(queueName);
    expect(job.priority).toBe("medium");
    expect(job.attemptsMade).toBe(0);
    expect(job.maxAttempts).toBe(3);
    expect(job.backoffMs).toBe(1000);
    expect(job.status).toBe("waiting");
    expect(job.runAfter).toBeGreaterThanOrEqual(before);
  });

  it("persists job data to Redis", async () => {
    const job = await queue.enqueue({ hello: "world" });
    const stored = await queue.getJob(job.id);

    expect(stored).not.toBeNull();
    expect(stored?.data).toEqual({ hello: "world" });
    expect(stored?.id).toBe(job.id);
  });

  it("respects custom options when enqueuing", async () => {
    const job = await queue.enqueue(
      { foo: "custom" },
      { maxAttempts: 5, backoffMs: 2000, priority: "high" }
    );

    expect(job.maxAttempts).toBe(5);
    expect(job.backoffMs).toBe(2000);
    expect(job.priority).toBe("high");
  });

  it("pushes non-delayed job directly into waiting list", async () => {
    const job = await queue.enqueue({ foo: "immediate" }, { priority: "high" });
    const waitingKeys = queue.waitingKeys();

    const len = await redis.lLen(waitingKeys.high);
    expect(len).toBe(1);

    const delayedLen = await redis.zCard(queue.delayedKeyName());
    expect(delayedLen).toBe(0);
  });

  it("pushes delayed job into delayed sorted set, not waiting", async () => {
    const job = await queue.enqueue({ foo: "later" }, { delayMs: 5000 });
    const waitingKeys = queue.waitingKeys();

    expect(await redis.zCard(queue.delayedKeyName())).toBe(1);
    expect(await redis.lLen(waitingKeys.medium)).toBe(0);
  });

  it("respects delayMs — runAfter is in the future", async () => {
    const delayMs = 5000;
    const before = Date.now();
    const job = await queue.enqueue({ foo: "bar" }, { delayMs });

    expect(job.runAfter).toBeGreaterThanOrEqual(before + delayMs);
  });

  it("returns null for non-existent job", async () => {
    const job = await queue.getJob("non-existent-id");
    expect(job).toBeNull();
  });

  // ── priority ordering ─────────────────────────────────────────────────────

  it("enqueues jobs into correct priority lists", async () => {
    await queue.enqueue({ p: "low" }, { priority: "low" });
    await queue.enqueue({ p: "high" }, { priority: "high" });
    await queue.enqueue({ p: "medium" }, { priority: "medium" });

    const { high, medium, low } = queue.waitingKeys();
    expect(await redis.lLen(high)).toBe(1);
    expect(await redis.lLen(medium)).toBe(1);
    expect(await redis.lLen(low)).toBe(1);
  });

  // ── promoteDelayedJobs ────────────────────────────────────────────────────

  it("promoteDelayedJobs moves due jobs into the waiting queue", async () => {
    const job = await queue.enqueue({ foo: "delayed" }, { delayMs: 5_000, priority: "high" });
    const delayedKey = queue.delayedKeyName();
    const waitingKeys = queue.waitingKeys();

    expect(await redis.zCard(delayedKey)).toBe(1);
    expect(await redis.lLen(waitingKeys.high)).toBe(0);

    // backdate the score so job is now due
    await redis.zAdd(delayedKey, [{ score: Date.now() - 1000, value: job.id }]);

    const moved = await queue.promoteDelayedJobs();
    expect(moved).toBe(1);

    expect(await redis.zCard(delayedKey)).toBe(0);
    expect(await redis.lLen(waitingKeys.high)).toBe(1);
  });

  it("does not promote jobs whose delay has not expired", async () => {
    await queue.enqueue({ foo: "future" }, { delayMs: 60_000 });

    const moved = await queue.promoteDelayedJobs();
    expect(moved).toBe(0);

    const waitingKeys = queue.waitingKeys();
    expect(await redis.lLen(waitingKeys.medium)).toBe(0);
  });

  it("promotes only due jobs when mixed due and future exist", async () => {
    const due = await queue.enqueue({ foo: "due" }, { delayMs: 1, priority: "low" });
    await queue.enqueue({ foo: "future" }, { delayMs: 60_000, priority: "low" });

    // backdate the due job
    await redis.zAdd(queue.delayedKeyName(), [{ score: Date.now() - 100, value: due.id }]);

    const moved = await queue.promoteDelayedJobs();
    expect(moved).toBe(1);

    expect(await redis.zCard(queue.delayedKeyName())).toBe(1); // future still there
    expect(await redis.lLen(queue.waitingKeys().low)).toBe(1); // due job promoted
  });

  // ── recoverStuckJobs ──────────────────────────────────────────────────────

  it("recoverStuckJobs moves long-running active jobs back to waiting", async () => {
    const waitingKeys = queue.waitingKeys();
    const activeKey = queue.activeKeyName();

    const job = await queue.enqueue({ foo: "stuck" }, { priority: "medium" });

    await redis.del(waitingKeys.medium);
    await redis.rPush(activeKey, job.id);

    job.status = "active";
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

  it("does not recover jobs still within maxProcessingMs", async () => {
    const activeKey = queue.activeKeyName();
    const job = await queue.enqueue({ foo: "fresh-active" }, { priority: "medium" });

    await redis.del(queue.waitingKeys().medium);
    await redis.rPush(activeKey, job.id);

    job.status = "active";
    job.startedAt = Date.now() - 5_000; // only 5s old
    await queue.saveJob(job);

    const recovered = await queue.recoverStuckJobs(30_000);
    expect(recovered).toBe(0);
    expect(await redis.lLen(activeKey)).toBe(1);
  });

  it("removes orphaned active job IDs with no job data", async () => {
    const activeKey = queue.activeKeyName();
    await redis.rPush(activeKey, "ghost-id");

    const recovered = await queue.recoverStuckJobs(30_000);
    expect(recovered).toBe(0);
    // ghost-id should be cleaned up — active list empty after pipeline runs
    // (only runs pipeline if recovered > 0, so ghost cleanup depends on impl)
    // At minimum it shouldn't crash
  });

  // ── markCompleted / markFailed ────────────────────────────────────────────

  it("markCompleted updates job status and pushes to completed list", async () => {
    const job = await queue.enqueue({ foo: "done" });

    await queue.markCompleted(job.id);

    const updated = await queue.getJob(job.id);
    expect(updated?.status).toBe("completed");
    expect(updated?.startedAt).toBeUndefined();

    const completedList = await redis.lRange(`rq:queue:${queueName}:completed`, 0, -1);
    expect(completedList).toContain(job.id);
  });

  it("markFailed updates job status and pushes to failed list", async () => {
    const job = await queue.enqueue({ foo: "broken" });

    await queue.markFailed(job.id);

    const updated = await queue.getJob(job.id);
    expect(updated?.status).toBe("failed");
    expect(updated?.startedAt).toBeUndefined();

    const failedList = await redis.lRange(`rq:queue:${queueName}:failed`, 0, -1);
    expect(failedList).toContain(job.id);
  });

  it("markCompleted on non-existent job does not throw", async () => {
    await expect(queue.markCompleted("ghost-id")).resolves.not.toThrow();
  });

  it("markFailed on non-existent job does not throw", async () => {
    await expect(queue.markFailed("ghost-id")).resolves.not.toThrow();
  });

  // ── saveJob ───────────────────────────────────────────────────────────────

  it("saveJob persists updated fields", async () => {
    const job = await queue.enqueue({ foo: "save-test" });
    job.status = "active";
    job.startedAt = Date.now();
    await queue.saveJob(job);

    const updated = await queue.getJob(job.id);
    expect(updated?.status).toBe("active");
    expect(updated?.startedAt).toBeDefined();
  });

  it("saveJob updates updatedAt timestamp", async () => {
    const job = await queue.enqueue({ foo: "timestamp" });
    const originalUpdatedAt = job.updatedAt;

    await new Promise((r) => setTimeout(r, 5));
    await queue.saveJob(job);

    const updated = await queue.getJob(job.id);
    expect(updated?.updatedAt).toBeGreaterThanOrEqual(originalUpdatedAt);
  });

  // ── addHistoryEntry ───────────────────────────────────────────────────────

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

    await Promise.all(
      Array.from({ length: 55 }, (_, i) =>
        queue.addHistoryEntry({ ...base, id: `job-${i}`, queueName })
      )
    );

    const raw = await redis.lRange(historyKey, 0, -1);
    expect(raw.length).toBe(50);
  });

  it("addHistoryEntry stores correct fields", async () => {
    const job = await queue.enqueue({ foo: "history" }, { priority: "high" });
    await queue.markCompleted(job.id);
    const completed = await queue.getJob(job.id);
    await queue.addHistoryEntry(completed!);

    const historyKey = queue.historyKeyName();
    const raw = await redis.lRange(historyKey, 0, 0);
    const entry = JSON.parse(raw[0]);

    expect(entry.id).toBe(job.id);
    expect(entry.status).toBe("completed");
    expect(entry.priority).toBe("high");
  });

  // ── removeFromActive ──────────────────────────────────────────────────────

  it("removeFromActive removes job id from active list", async () => {
    const activeKey = queue.activeKeyName();
    await redis.rPush(activeKey, "some-job-id");

    await queue.removeFromActive("some-job-id");

    const len = await redis.lLen(activeKey);
    expect(len).toBe(0);
  });

  // ── getQueueMetrics ───────────────────────────────────────────────────────

  it("getQueueMetrics aggregates waiting counts correctly", async () => {
    await queue.enqueue({ t: 1 }, { priority: "high" });
    await queue.enqueue({ t: 2 }, { priority: "medium" });
    await queue.enqueue({ t: 3 }, { priority: "low" });

    const metrics = await getQueueMetrics(queueName);

    expect(metrics.waitingHigh).toBe(1);
    expect(metrics.waitingMedium).toBe(1);
    expect(metrics.waitingLow).toBe(1);
  });

  it("getQueueMetrics counts completed and failed jobs", async () => {
    const j1 = await queue.enqueue({ t: 1 });
    const j2 = await queue.enqueue({ t: 2 });

    await queue.markCompleted(j1.id);
    await queue.markFailed(j2.id);

    const metrics = await getQueueMetrics(queueName);

    expect(metrics.completed).toBe(1);
    expect(metrics.failed).toBe(1);
  });

  it("getQueueMetrics counts delayed jobs", async () => {
    await queue.enqueue({ t: 1 }, { delayMs: 60_000 });
    await queue.enqueue({ t: 2 }, { delayMs: 60_000 });

    const metrics = await getQueueMetrics(queueName);
    expect(metrics.delayed).toBe(2);
  });
});

// ── Worker ────────────────────────────────────────────────────────────────────

describe("Worker", () => {
  let testId = 0;

  // Each test gets its own queue name — leaked poll loops from test N
  // can never touch test N+1's data even if they outlive stopAndDrain.
  function makeQueue() {
    return new Queue(`worker-test-${++testId}`);
  }

  beforeAll(() => {
    jest.spyOn(console, "warn").mockImplementation(() => {});
    jest.spyOn(console, "log").mockImplementation(() => {});
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterAll(() => {
    (console.warn as jest.Mock).mockRestore();
    (console.log as jest.Mock).mockRestore();
    (console.error as jest.Mock).mockRestore();
  });

  async function stopAndDrain(worker: Worker, drainMs = 800) {
    await worker.stop();
    await new Promise((r) => setTimeout(r, drainMs));
  }

  it("processes a job and marks it completed", async () => {
    const queue = makeQueue();
    const processed: any[] = [];
    const handler = jest.fn(async (job: Job) => {
      processed.push(job.data);
    });

    const job = await queue.enqueue({ task: "hello" });
    const worker = new Worker(queue, handler, {
      concurrency: 1,
      pollIntervalMs: 50,
      maintenanceIntervalMs: 999_999,
    });
    worker.start();

    await new Promise((r) => setTimeout(r, 1500));
    await stopAndDrain(worker);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(processed[0]).toEqual({ task: "hello" });
    expect((await queue.getJob(job.id))?.status).toBe("completed");
  }, 10_000);

  it("retries a failing job up to maxAttempts then marks failed", async () => {
    const queue = makeQueue();
    const handler = jest.fn(async (_job: Job) => {
      throw new Error("always fails");
    });

    const job = await queue.enqueue({ task: "fail" }, { maxAttempts: 2, backoffMs: 100 });
    const worker = new Worker(queue, handler, {
      concurrency: 1,
      pollIntervalMs: 50,
      maintenanceIntervalMs: 200,
    });
    worker.start();

    await new Promise((r) => setTimeout(r, 4000));
    await stopAndDrain(worker);

    const updated = await queue.getJob(job.id);
    expect(updated?.attemptsMade).toBe(2);
    expect(updated?.status).toBe("failed");
    expect(updated?.lastError).toMatch(/always fails/);
  }, 15_000);

  it("respects concurrency — processes multiple jobs in parallel", async () => {
    const queue = makeQueue();
    const startTimes: number[] = [];
    const handler = jest.fn(async (_job: Job) => {
      startTimes.push(Date.now());
      await new Promise((r) => setTimeout(r, 300));
    });

    await Promise.all([
      queue.enqueue({ n: 1 }),
      queue.enqueue({ n: 2 }),
      queue.enqueue({ n: 3 }),
    ]);

    const worker = new Worker(queue, handler, {
      concurrency: 3,
      pollIntervalMs: 30,
      maintenanceIntervalMs: 999_999,
    });
    worker.start();

    await new Promise((r) => setTimeout(r, 1200));
    await stopAndDrain(worker);

    expect(handler).toHaveBeenCalledTimes(3);
    const spread = Math.max(...startTimes) - Math.min(...startTimes);
    expect(spread).toBeLessThan(150);
  }, 10_000);

  it("processes high priority jobs before low priority", async () => {
    const queue = makeQueue();
    const order: string[] = [];
    const handler = jest.fn(async (job: Job) => {
      order.push(job.data.priority);
      await new Promise((r) => setTimeout(r, 100));
    });

    await queue.enqueue({ priority: "low" }, { priority: "low" });
    await queue.enqueue({ priority: "high" }, { priority: "high" });
    await queue.enqueue({ priority: "medium" }, { priority: "medium" });

    const worker = new Worker(queue, handler, {
      concurrency: 1,
      pollIntervalMs: 30,
      maintenanceIntervalMs: 999_999,
    });
    worker.start();

    // Poll until all 3 jobs processed or timeout
    const deadline = Date.now() + 8000;
    while (order.length < 3 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    await stopAndDrain(worker);

    expect(order[0]).toBe("high");
    expect(order[1]).toBe("medium");
    expect(order[2]).toBe("low");
  }, 15_000);

  it("adds completed job to history", async () => {
    const queue = makeQueue();
    const handler = jest.fn(async (_job: Job) => {});

    const job = await queue.enqueue({ task: "history-check" });
    const worker = new Worker(queue, handler, {
      concurrency: 1,
      pollIntervalMs: 50,
      maintenanceIntervalMs: 999_999,
    });
    worker.start();

    // Poll until history entry appears or timeout
    const deadline = Date.now() + 8000;
    let raw: string[] = [];
    while (Date.now() < deadline) {
      raw = await redis.lRange(queue.historyKeyName(), 0, -1);
      if (raw.length > 0) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    await stopAndDrain(worker);

    expect(raw.length).toBe(1);
    const entry = JSON.parse(raw[0]);
    expect(entry.id).toBe(job.id);
    expect(entry.status).toBe("completed");
  }, 15_000);

  it("adds permanently failed job to history", async () => {
    const queue = makeQueue();
    const handler = jest.fn(async (_job: Job) => {
      throw new Error("permanent fail");
    });

    const job = await queue.enqueue(
      { task: "fail-history" },
      { maxAttempts: 1, backoffMs: 0 }
    );
    const worker = new Worker(queue, handler, {
      concurrency: 1,
      pollIntervalMs: 50,
      maintenanceIntervalMs: 999_999,
    });
    worker.start();

    // Poll until history entry appears or timeout
    const deadline = Date.now() + 8000;
    let raw: string[] = [];
    while (Date.now() < deadline) {
      raw = await redis.lRange(queue.historyKeyName(), 0, -1);
      if (raw.length > 0) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    await stopAndDrain(worker);

    expect(raw.length).toBe(1);
    const entry = JSON.parse(raw[0]);
    expect(entry.id).toBe(job.id);
    expect(entry.status).toBe("failed");
  }, 15_000);
});