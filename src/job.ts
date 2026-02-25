import { FastifyInstance } from "fastify";
import { redis } from "./redisClient";
import { getOrCreateQueue } from "./server";

export async function jobRoutes(fastify: FastifyInstance) {
  fastify.get("/jobs", async (request, reply) => {
    try {
      const query = request.query as { queueName?: string; limit?: string };
      const queueName = query.queueName || "default";
      const limit = Math.max(1, Math.min(parseInt(query.limit || "15", 10), 100));

      const queue = getOrCreateQueue(queueName);
      const historyKey = queue.historyKeyName();

      const raw = await redis.lRange(historyKey, 0, limit - 1);
      const jobs = raw
        .map((row) => {
          try {
            return JSON.parse(row);
          } catch {
            return null;
          }
        })
        .filter(Boolean);

      return reply.send({ queueName, jobs });
    } catch (err: any) {
      fastify.log.error({ err }, "[/jobs] Error");
      return reply.status(500).send({ error: String(err.message ?? err) });
    }
  });
}