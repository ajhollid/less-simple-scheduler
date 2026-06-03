import { EventEmitter } from "events";
import { IJob, JobId } from "../job/types.js";
import { BulkWriteResult } from "../store/types.js";

export interface AddJobInput {
  id?: JobId;
  template: string;
  startAt?: number;
  repeat?: number;
  data?: unknown;
  active?: boolean;
  jitter?: number | boolean;
  upsert?: boolean;
}

export interface IScheduler extends EventEmitter {
  emit<K extends keyof SchedulerEvents>(
    event: K,
    ...args: Parameters<SchedulerEvents[K]>
  ): boolean;

  on<K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ): this;

  once<K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ): this;

  off<K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ): this;

  removeListener<K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ): this;

  connect: () => Promise<boolean>;
  disconnect: () => Promise<boolean>;
  start: () => Promise<boolean>;
  stop: () => Promise<boolean>;
  processJobs(): Promise<void>;

  addJob: (input: AddJobInput) => Promise<IJob>;

  addJobs: (inputs: AddJobInput[]) => Promise<BulkWriteResult>;

  pauseJob(id: JobId): Promise<boolean>;

  resumeJob(id: JobId): Promise<boolean>;

  getJob(id: JobId): Promise<IJob | null>;

  getJobs(): Promise<IJob[]>;

  removeJob(id: JobId): Promise<boolean>;

  updateJob(id: JobId, updates: Partial<IJob>): Promise<boolean>;

  flushJobs(): Promise<boolean>;

  addTemplate(
    name: string,
    template: (data?: any) => void | Promise<void>,
  ): Promise<boolean>;

  getTemplates(): Promise<Array<(data?: any) => void | Promise<void>>>;

  removeTemplate(name: string): Promise<boolean>;
}

export type SchedulerOptions = {
  concurrency?: number;
  processEvery?: number;
  lockMs?: number;
  drainTimeoutMs?: number;
};

export interface SchedulerEvents {
  // Scheduler lifecycle — every event carries the originating workerId
  // as its first argument so listeners can tell instances apart.
  "scheduler:start": (workerId: string) => void;
  "scheduler:stop": (workerId: string) => void;
  "scheduler:drain": (workerId: string, count: number) => void;
  "scheduler:error": (workerId: string, error: Error) => void;
  "scheduler:heartbeat": (workerId: string) => void;

  // Job lifecycle
  "job:locked": (workerId: string, job: IJob) => void;
  "job:start": (workerId: string, job: IJob) => void;
  "job:attempt": (workerId: string, job: IJob, attempt: number) => void;
  "job:complete": (workerId: string, job: IJob) => void;
  "job:fail": (
    workerId: string,
    job: IJob,
    error: unknown,
    attempt: number,
  ) => void;
  "job:exhausted": (workerId: string, job: IJob, error: unknown) => void;
  "job:abort": (workerId: string, job: IJob, reason: string) => void;
}
