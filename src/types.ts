// src/types.ts

export type JobStatus = "waiting" | "active" | "completed" | "failed";
export type Priority = "high" | "medium" | "low";

export interface JobData {
  [key: string]: any;
}

export interface Job {
  id: string;
  queueName: string;
  data: JobData;
  attemptsMade: number;
  maxAttempts: number;
  backoffMs: number;
  createdAt: number;
  updatedAt: number;
  runAfter: number;
  status: JobStatus;
  priority: Priority;
  startedAt?: number;
  lastError?: string;
}

export interface EnqueueOptions {
  delayMs?: number;
  maxAttempts?: number;
  backoffMs?: number;
  priority?: Priority;
}

export type JobHandler = (job: Job) => Promise<void>;
