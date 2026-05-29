import { EventEmitter } from "events";
import { hostname } from "node:os";
import { randomUUID } from "node:crypto";

import type { IScheduler, SchedulerEvents, SchedulerOptions } from "./types.js";
import { IStore } from "../store/types.js";
import { IJob, JobId } from "../job/types.js";

type Template = (data?: any) => void | Promise<void>;

interface InFlightEntry {
  job: IJob;
  lostLock: boolean;
  promise?: Promise<void>;
}

const DEFAULTS = {
  concurrency: 50,
  processEvery: 1000,
  lockMs: 15_000,
  drainTimeoutMs: 5_000,
  maxAttempts: 5,
  backoffMs: 1000,
} as const;

const MIN_HEARTBEAT_MS = 100;

export class Scheduler extends EventEmitter implements IScheduler {
  declare emit: <K extends keyof SchedulerEvents>(
    event: K,
    ...args: Parameters<SchedulerEvents[K]>
  ) => boolean;
  declare on: <K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ) => this;
  declare once: <K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ) => this;
  declare off: <K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ) => this;
  declare removeListener: <K extends keyof SchedulerEvents>(
    event: K,
    listener: SchedulerEvents[K],
  ) => this;

  public readonly workerId: string;
  private readonly templates: Map<string, Template> = new Map<
    string,
    Template
  >();
  private connected: boolean = false;
  private started: boolean = false;
  private processJobIntervalId: NodeJS.Timeout | null = null;
  private lockMs: number;
  private heartbeatId: NodeJS.Timeout | null = null;
  private processEvery: number;
  private concurrency: number;
  private inFlightJobs: Map<JobId, InFlightEntry>;
  private drainTimeoutMs: number;
  constructor(
    private store: IStore,
    options: SchedulerOptions,
  ) {
    super();
    this.workerId = `${hostname()}:${process.pid}:${randomUUID()}`;
    this.lockMs = options.lockMs ?? DEFAULTS.lockMs;
    this.processEvery = options.processEvery ?? DEFAULTS.processEvery;
    this.concurrency = options.concurrency ?? DEFAULTS.concurrency;
    this.inFlightJobs = new Map<JobId, InFlightEntry>();
    this.drainTimeoutMs = options.drainTimeoutMs ?? DEFAULTS.drainTimeoutMs;
  }

  private emitSchedulerError(err: unknown): void {
    this.emit(
      "scheduler:error",
      this.workerId,
      err instanceof Error ? err : new Error(String(err)),
    );
  }

  private async heartbeatTick(): Promise<void> {
    const snapshot = Array.from(this.inFlightJobs);
    await Promise.allSettled(
      snapshot.map(async ([id, entry]) => {
        if (entry.lostLock) return;
        try {
          // This will renew the lock as long as it hasn't been lost
          const ok = await this.store.heartbeat(id, this.workerId, this.lockMs);
          if (!ok && !entry.lostLock) {
            entry.lostLock = true;
            this.emit("job:abort", this.workerId, entry.job, "lock lost");
          }
        } catch {
          // store error (e.g. during shutdown) — ignore
        }
      }),
    );
  }

  connect = async () => {
    if (this.connected) return false;
    await this.store.connect();
    this.connected = true;
    return true;
  };

  disconnect = async () => {
    if (!this.connected) return false;
    if (this.started) {
      throw new Error(
        "Cannot disconnect while scheduler is started. Call stop() first.",
      );
    }
    await this.store.disconnect();
    this.connected = false;
    return true;
  };

  start = async () => {
    // If the scheduler is already started, emit an error event and return false
    if (this.started) {
      this.emitSchedulerError(new Error("Scheduler is already started."));
      return false;
    }

    // ***********************************************
    // 1.  Connect the store
    // ***********************************************
    const connected = await this.connect();
    if (!connected) {
      this.emitSchedulerError(new Error("Failed to connect to the store."));
      return false;
    }
    this.started = true;

    // ***********************************************
    // 2.  Start processing jobs
    // ***********************************************
    this.processJobIntervalId = setInterval(() => {
      this.processJobs().catch((err) => this.emitSchedulerError(err));
    }, this.processEvery);

    // ***********************************************
    // 3.  Start heartbeat to renew locks
    // ***********************************************
    const heartbeatEvery = Math.max(
      MIN_HEARTBEAT_MS,
      Math.floor(this.lockMs / 3), // heartbeat at least 3 times during the lock duration
    );
    this.heartbeatId = setInterval(() => {
      this.heartbeatTick().catch((err) => this.emitSchedulerError(err));
    }, heartbeatEvery);

    // ***********************************************
    // 4.  Mark as started
    // ***********************************************
    this.emit("scheduler:start", this.workerId);
    return true;
  };

  stop = async () => {
    if (!this.started) {
      this.emitSchedulerError(new Error("Scheduler is not started."));
      return false;
    }

    this.started = false;

    // ***********************************************
    // 1.  Stop processing jobs, clear interval
    // ***********************************************
    if (this.processJobIntervalId) {
      clearInterval(this.processJobIntervalId);
      this.processJobIntervalId = null;
    }

    // ***********************************************
    // 2.  Drain in flight jobs.  Heartbeat still active to renew locks at this point
    // ***********************************************
    const inFlight: Promise<void>[] = [];
    for (const entry of this.inFlightJobs.values()) {
      if (entry.promise) inFlight.push(entry.promise);
    }

    if (inFlight.length > 0) {
      let completed = 0;
      for (const p of inFlight) p.finally(() => completed++); // Attach a callback to each promise

      // Race a drain timeout against completion of all in flight jobs
      await Promise.race([
        Promise.allSettled(inFlight),
        new Promise<void>((resolve) => {
          setTimeout(resolve, this.drainTimeoutMs);
        }),
      ]);
      this.emit("scheduler:drain", this.workerId, completed);
    }

    // ***********************************************
    // 3.  Stop the heartbeat after attempting to drain
    // ***********************************************
    if (this.heartbeatId) {
      clearInterval(this.heartbeatId);
      this.heartbeatId = null;
    }

    // ***********************************************
    // 4.  Emit abort for any jobs that couldn't be drained
    // ***********************************************
    for (const [, entry] of this.inFlightJobs) {
      if (!entry.lostLock) {
        // Don't emit for jobs that have lost a lock, they would have already been emitted as aborted in the heartbeat tick
        this.emit("job:abort", this.workerId, entry.job, "scheduler-stopped");
      }
    }

    // ***********************************************
    // 5.  Disconnect and emit stop
    // ***********************************************
    await this.disconnect();
    this.emit("scheduler:stop", this.workerId);
    return true;
  };

  // ***********************************************
  // This is the actual tick
  // ***********************************************

  private async runJob(job: IJob, entry: InFlightEntry): Promise<void> {
    const template = this.templates.get(job.template);

    // ***********************************************
    // 1.  Return early if there's no template and lock was lost to prevent double emit
    // ***********************************************
    if (!template && entry.lostLock) return;

    // ***********************************************
    // 2.  Return early if there's no template, mark job as failed and release lock
    // ***********************************************
    if (!template) {
      const error = new Error(`Unknown template: ${job.template}`);
      const now = Date.now();
      const released = await this.store.releaseLock(job.id, this.workerId, {
        lastResult: "error",
        lastError: error.message,
        lastFinishedAt: now,
        lastFailedAt: now,
        failCount: (job.failCount ?? 0) + 1,
      });
      if (!released) {
        this.emit("job:abort", this.workerId, job, "lock lost");
      } else {
        this.emit("job:fail", this.workerId, job, error, job.attempts + 1);
      }
      return;
    }

    // ***********************************************
    // 3.  Attempt job
    // ***********************************************
    const attempt = job.attempts + 1;
    this.emit("job:attempt", this.workerId, job, attempt);
    this.emit("job:start", this.workerId, job);

    try {
      // ***********************************************
      // 3a.  Attempt the actual job here
      // ***********************************************
      await template(job.data);
      if (entry.lostLock) return;

      // ***********************************************
      // 3b.  Recurring jobs: advance the schedule atomically in the
      //      store so the next run honours the job's *live* repeat
      // ***********************************************
      const now = Date.now();
      if (job.repeat > 0) {
        const updated = await this.store.releaseRecurring(
          job.id,
          this.workerId,
          now,
        );
        if (!updated) {
          this.emit("job:abort", this.workerId, job, "lock lost");
          return;
        }
        this.emit("job:complete", this.workerId, updated);
        return;
      }

      // ***********************************************
      // 3c.  One-shot jobs: deactivate and release the lock.
      // ***********************************************
      const patch: Partial<IJob> = {
        active: false,
        lastFinishedAt: now,
        lastResult: "ok",
        lastError: null,
        attempts: 0,
        runCount: (job.runCount ?? 0) + 1,
      };
      const released = await this.store.releaseLock(
        job.id,
        this.workerId,
        patch,
      );
      if (!released) {
        this.emit("job:abort", this.workerId, job, "lock lost");
        return;
      }
      this.emit("job:complete", this.workerId, { ...job, ...patch });
    } catch (error: unknown) {
      if (entry.lostLock) return;

      const now = Date.now();
      const message = error instanceof Error ? error.message : String(error);
      const exhausted = attempt >= job.maxAttempts;
      const backoff = job.backoffMs * 2 ** job.attempts;
      const failCount = (job.failCount ?? 0) + 1;

      const patch: Partial<IJob> = exhausted
        ? {
            active: false,
            attempts: attempt,
            lastResult: "error",
            lastError: message,
            lastFinishedAt: now,
            lastFailedAt: now,
            failCount,
          }
        : {
            attempts: attempt,
            nextRunAt: now + backoff,
            lastResult: "error",
            lastError: message,
            lastFinishedAt: now,
            lastFailedAt: now,
            failCount,
          };
      const released = await this.store.releaseLock(
        job.id,
        this.workerId,
        patch,
      );
      if (!released) {
        this.emit("job:abort", this.workerId, job, "lock lost");
        return;
      }

      if (exhausted) {
        this.emit("job:exhausted", this.workerId, job, error);
      } else {
        this.emit("job:fail", this.workerId, job, error, attempt);
      }
    }
  }

  processJobs = async () => {
    while (this.started && this.inFlightJobs.size < this.concurrency) {
      // ***********************************************
      // 1.  Claim the job
      // ***********************************************
      const job = await this.store.claimDueJob(this.workerId, this.lockMs);
      if (!job) return;

      // ***********************************************
      // 1a.  Lock acquired — emit to see which worker won the job
      // ***********************************************
      this.emit("job:locked", this.workerId, job);

      // ***********************************************
      // 2.  Add to in flight jobs
      // ***********************************************
      const entry: InFlightEntry = { job, lostLock: false };
      this.inFlightJobs.set(job.id, entry);

      // ***********************************************
      // 3.  Run the job, remove from in flight when done
      // IMPORTANT:  Must remain non blocking DO NOT AWAIT
      // ***********************************************
      entry.promise = this.runJob(job, entry)
        .catch((error) => this.emitSchedulerError(error))
        .finally(() => {
          this.inFlightJobs.delete(job.id);
        });
    }
  };

  // ***********************************************
  // Job CRUD
  // ***********************************************

  addJob = async (input: {
    id?: JobId;
    template: string;
    startAt?: number;
    repeat?: number;
    data?: unknown;
    active?: boolean;
    jitter?: number | boolean;
    upsert?: boolean;
  }): Promise<IJob> => {
    // ***********************************************
    // 1.  Set up timing
    // ***********************************************
    const now = Date.now();
    const base = input.startAt ?? now;
    const repeat = input.repeat ?? 0;

    // ***********************************************
    // 1a.  Add jitter to spread out start times
    // ***********************************************
    let jitterMs = 0;
    if (input.jitter === true) {
      jitterMs = repeat;
    } else if (typeof input.jitter === "number" && input.jitter > 0) {
      jitterMs = input.jitter;
    }
    const startAt = base + (jitterMs > 0 ? Math.random() * jitterMs : 0);

    // ***********************************************
    // 2.  Create job
    // ***********************************************
    const job: IJob = {
      id: input.id ?? randomUUID(),
      template: input.template,
      data: input.data,
      startAt,
      repeat,
      active: input.active ?? true,

      nextRunAt: startAt,
      lastFinishedAt: null,
      lastScheduledAt: startAt,
      lastResult: null,
      lastError: null,
      lastStartedAt: null,
      lastFailedAt: null,

      lockedBy: null,
      lockedUntil: null,
      lockedAt: null,

      attempts: 0,
      maxAttempts: DEFAULTS.maxAttempts,
      backoffMs: DEFAULTS.backoffMs,

      runCount: 0,
      failCount: 0,

      createdAt: now,
      updatedAt: now,
    };

    // ***********************************************
    // 3.  Upsert if requested, otherwise insert
    // ***********************************************
    if (input.upsert) {
      const setOnInsert: Partial<IJob> = {
        nextRunAt: job.nextRunAt,
        lastFinishedAt: job.lastFinishedAt,
        lastScheduledAt: job.lastScheduledAt,
        lastResult: job.lastResult,
        lastError: job.lastError,
        lastStartedAt: job.lastStartedAt,
        lastFailedAt: job.lastFailedAt,
        lockedBy: job.lockedBy,
        lockedUntil: job.lockedUntil,
        lockedAt: job.lockedAt,
        attempts: job.attempts,
        runCount: job.runCount,
        failCount: job.failCount,
        createdAt: job.createdAt,
      };
      const set: Partial<IJob> = {
        template: job.template,
        data: job.data,
        startAt: job.startAt,
        repeat: job.repeat,
        active: job.active,
        maxAttempts: job.maxAttempts,
        backoffMs: job.backoffMs,
        updatedAt: job.updatedAt,
      };
      return await this.store.upsert(job.id, setOnInsert, set);
    }
    return await this.store.insert(job);
  };

  async pauseJob(id: JobId): Promise<boolean> {
    return await this.store.setActive(id, false);
  }

  async resumeJob(id: JobId): Promise<boolean> {
    return await this.store.setActive(id, true);
  }

  async getJob(id: JobId): Promise<IJob | null> {
    return await this.store.get(id);
  }

  async getJobs(): Promise<IJob[]> {
    return await this.store.list();
  }

  async removeJob(id: JobId): Promise<boolean> {
    return await this.store.remove(id);
  }

  async updateJob(id: JobId, updates: Partial<IJob>): Promise<boolean> {
    const updated = await this.store.update(id, updates);
    return updated !== null;
  }

  async flushJobs(): Promise<boolean> {
    await this.store.removeAll();
    return true;
  }

  // ***********************************************
  // Templates
  // ***********************************************
  async addTemplate(name: string, template: Template): Promise<boolean> {
    if (this.templates.has(name)) return false;
    this.templates.set(name, template);
    return true;
  }

  async getTemplates(): Promise<Template[]> {
    return Array.from(this.templates.values());
  }

  async removeTemplate(name: string): Promise<boolean> {
    return this.templates.delete(name);
  }
}
