
import { FastifyInstance } from "fastify";
import { getOrCreateQueue } from "./server";
import { getQueueMetrics } from "./metrics";

export async function metricsRoutes(fastify: FastifyInstance) {
  fastify.get("/metrics", async (request, reply) => {
    try {
      const query = request.query as { queueName?: string };
      const queueName = query.queueName || "default";

      // ensure queue + worker exist
      getOrCreateQueue(queueName);

      // compute metrics using your helper
      const metrics = await getQueueMetrics(queueName);

      return reply.send(metrics);
    } catch (err: any) {
      fastify.log.error({ err }, "[/metrics] Error");
      return reply.status(500).send({ error: String(err.message ?? err) });
    }
  });
}