export type JobId = string | number;

export type JobResult = "ok" | "error";

export interface IJob {
  id: JobId;
  template: string;
  data?: unknown;

  // User-configured schedule
  startAt: number; // ms epoch - first scheduled run
  repeat: number; // ms interval; 0 = one-shot
  active: boolean;

  // Scheduler-managed schedule state
  nextRunAt: number;
  lastRunAt: number | null; // last completion time (set on release)
  lastScheduledAt: number;
  lastResult: JobResult | null;
  lastError: string | null;
  lastStartedAt: number | null; // when the most recent run started (set on claim)
  lastFailedAt: number | null; // when the most recent failure was recorded

  // Lock state (held while a worker is running this job)
  lockedBy: string | null;
  lockedUntil: number | null;
  lockedAt: number | null; // ms epoch - set on lock, cleared on release; non-null = running

  // Retry state
  attempts: number; // consecutive failed attempts (resets to 0 on success)
  maxAttempts: number;
  backoffMs: number;

  // Lifetime counters
  runCount: number; // total successful runs over the job's lifetime
  failCount: number; // total failed runs over the job's lifetime

  createdAt: number;
  updatedAt: number;
}
