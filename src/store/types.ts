import type { IJob, JobId } from "../job/types.js";

export interface ListOptions {
  skip?: number;
  limit?: number;
}

/**
 * A single operation in a bulk write: either a plain insert of a fully
 * built job, or an upsert by id with the same setOnInsert/set split as
 * the single-job {@link IStore.upsert}.
 */
export type BulkOp =
  | { kind: "insert"; job: IJob }
  | {
      kind: "upsert";
      id: JobId;
      setOnInsert: Partial<IJob>;
      set: Partial<IJob>;
    };

export interface BulkFailure {
  id: JobId;
  error: string;
}

export interface BulkWriteResult {
  /** Brand-new documents written (plain inserts + upserts that inserted). */
  inserted: number;
  /** Existing documents modified by an upsert. */
  updated: number;
  /** Ops that failed (e.g. duplicate id on a plain insert), keyed by id. */
  failed: BulkFailure[];
}

export interface IStore {
  // ### Lifecycle ###
  connect(): Promise<void>;
  disconnect(): Promise<void>;

  // ### CRUD ###
  insert(job: IJob): Promise<IJob>;

  /**
   * Best-effort bulk write. Ops run unordered: every valid op is applied
   * even if others fail. Returns insert/update counts plus the ops that
   * failed and why. Never rejects on per-op write errors
   */
  bulkWrite(ops: BulkOp[]): Promise<BulkWriteResult>;
  /**
   * Upsert a job by id. `set` fields are applied unconditionally;
   * `setOnInsert` fields are only applied when the document doesn't
   * exist. Returns true if a new document was inserted, false if an
   * existing document was updated.
   */
  upsert(
    id: JobId,
    setOnInsert: Partial<IJob>,
    set: Partial<IJob>,
  ): Promise<IJob>;
  get(id: JobId): Promise<IJob | null>;
  list(options?: ListOptions): Promise<IJob[]>;
  count(): Promise<number>;
  update(id: JobId, updates: Partial<IJob>): Promise<IJob | null>;
  remove(id: JobId): Promise<boolean>;
  removeAll(): Promise<number>;
  setActive(id: JobId, active: boolean): Promise<boolean>;

  // ### Scheduling primitives (must be atomic) ###
  claimDueJob(workerId: string, lockMs: number): Promise<IJob | null>;

  /**
   * Bump `lockedUntil` for a currently locked job. Returns false if
   * the lock has been lost (no longer owned by `workerId`) — the
   * caller must then discard the in-flight result.
   */
  heartbeat(id: JobId, workerId: string, lockMs: number): Promise<boolean>;

  /**
   * Apply post-run patch (e.g. `nextRunAt`, `lastFinishedAt`, `attempts`,
   * `lastResult`, `lastError`) AND clear the lock, in a single atomic
   * write. Only succeeds if `lockedBy` still equals `workerId`.
   */
  releaseLock(
    id: JobId,
    workerId: string,
    patch: Partial<IJob>,
  ): Promise<boolean>;

  /**
   * The recurring-success counterpart to `releaseLock`: advances the
   * schedule AND clears the lock in a single atomic write.
   * `nextRunAt`/`lastScheduledAt` are advanced by the job's current
   * `repeat`, so a `repeat` change applied mid-flight takes effect on this same cycle.
   * Only succeeds if `lockedBy` still equals `workerId`.
   * Returns the updated job, or null if the lock has been lost.
   */
  releaseRecurring(
    id: JobId,
    workerId: string,
    now: number,
  ): Promise<IJob | null>;
}
