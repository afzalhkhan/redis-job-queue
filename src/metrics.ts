import { redis } from "./redisClient";

const KEY_PREFIX = process.env.REDIS_NAMESPACE ?? "rq:";

export async function getQueueMetrics(queueName: string) {
  const waitingHighKey = `${KEY_PREFIX}queue:${queueName}:waiting:high`;
  const waitingMediumKey = `${KEY_PREFIX}queue:${queueName}:waiting:medium`;
  const waitingLowKey = `${KEY_PREFIX}queue:${queueName}:waiting:low`;
  const delayedKey = `${KEY_PREFIX}queue:${queueName}:delayed`;
  const completedKey = `${KEY_PREFIX}queue:${queueName}:completed`;
  const failedKey = `${KEY_PREFIX}queue:${queueName}:failed`;
  const activeKey = `${KEY_PREFIX}queue:${queueName}:active`;

  const [
    waitingHigh,
    waitingMedium,
    waitingLow,
    delayed,
    completed,
    failed,
    active,
  ] = await Promise.all([
    redis.lLen(waitingHighKey),
    redis.lLen(waitingMediumKey),
    redis.lLen(waitingLowKey),
    redis.zCard(delayedKey),
    redis.lLen(completedKey),
    redis.lLen(failedKey),
    redis.lLen(activeKey),
  ]);

  const waiting = waitingHigh + waitingMedium + waitingLow;

  return {
    queueName,
    waiting,
    waitingHigh,
    waitingMedium,
    waitingLow,
    delayed,
    active,
    completed,
    failed,
  };
}