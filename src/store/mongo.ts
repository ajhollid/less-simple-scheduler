import { MongoClient, type Collection, type Filter } from "mongodb";
import type { IJob, JobId } from "../job/types.js";
import type { IStore } from "./types.js";

export interface MongoStoreOptions {
  // ***********************************************
  // url is full connection string including DB name
  // ***********************************************
  url: string;
  collection?: string;
}

// ***********************************************
// Map IJob.id <-> Mongo's _id so id lookups use the primary key
// ***********************************************
type MongoJob = Omit<IJob, "id"> & { _id: JobId };

const toMongo = (job: IJob): MongoJob => {
  const { id, ...rest } = job;
  return { _id: id, ...rest };
};

const fromMongo = (doc: MongoJob): IJob => {
  const { _id, ...rest } = doc;
  return { id: _id, ...rest };
};

const stripId = <T extends Partial<IJob>>(patch: T): Omit<T, "id"> => {
  const { id: _drop, ...rest } = patch;
  return rest;
};

const omitUndefined = <T extends object>(obj: T): Partial<T> => {
  const out: Partial<T> = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) (out as Record<string, unknown>)[key] = value;
  }
  return out;
};

export class MongoStore implements IStore {
  private readonly client: MongoClient;
  private readonly collectionName: string;
  private collection: Collection<MongoJob> | null = null;

  constructor(opts: MongoStoreOptions) {
    this.client = new MongoClient(opts.url);
    this.collectionName = opts.collection ?? "jobs";
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.collection = this.client
      .db()
      .collection<MongoJob>(this.collectionName);
    await this.ensureIndexes();
  }

  async disconnect(): Promise<void> {
    await this.client.close();
    this.collection = null;
  }

  private requireCollection(): Collection<MongoJob> {
    if (!this.collection) {
      throw new Error("MongoStore is not connected — call connect() first");
    }
    return this.collection;
  }

  private async ensureIndexes(): Promise<void> {
    await this.requireCollection().createIndexes([
      { key: { active: 1, nextRunAt: 1, lockedUntil: 1 } },
      { key: { lockedBy: 1, lockedUntil: 1 } },
      { key: { template: 1 } },
    ]);
  }

  // ***********************************************
  // CRUD
  // ***********************************************
  async insert(job: IJob): Promise<IJob> {
    await this.requireCollection().insertOne(toMongo(job));
    return job;
  }

  async upsert(
    id: JobId,
    setOnInsert: Partial<IJob>,
    set: Partial<IJob>,
  ): Promise<IJob> {
    const doc = await this.requireCollection().findOneAndUpdate(
      { _id: id },
      {
        $set: omitUndefined(stripId(set)),
        $setOnInsert: omitUndefined(stripId(setOnInsert)),
      },
      { upsert: true, returnDocument: "after" },
    );
    if (!doc) {
      throw new Error(`Failed to upsert job with id ${id}`);
    }
    return fromMongo(doc);
  }

  async get(id: JobId): Promise<IJob | null> {
    const doc = await this.requireCollection().findOne({ _id: id });
    return doc ? fromMongo(doc) : null;
  }

  async list(): Promise<IJob[]> {
    const docs = await this.requireCollection().find().toArray();
    return docs.map(fromMongo);
  }

  async update(id: JobId, updates: Partial<IJob>): Promise<IJob | null> {
    const doc = await this.requireCollection().findOneAndUpdate(
      { _id: id },
      { $set: { ...stripId(updates), updatedAt: Date.now() } },
      { returnDocument: "after" },
    );
    return doc ? fromMongo(doc) : null;
  }

  async remove(id: JobId): Promise<boolean> {
    const result = await this.requireCollection().deleteOne({ _id: id });
    return result.deletedCount === 1;
  }

  async removeAll(): Promise<number> {
    const result = await this.requireCollection().deleteMany({});
    return result.deletedCount;
  }

  async setActive(id: JobId, active: boolean): Promise<boolean> {
    const result = await this.requireCollection().updateOne(
      { _id: id },
      { $set: { active, updatedAt: Date.now() } },
    );
    return result.matchedCount === 1;
  }

  // ***********************************************
  // Scheduling primitives, must be atomic
  // ***********************************************
  async claimDueJob(workerId: string, lockMs: number): Promise<IJob | null> {
    const now = Date.now();
    const filter: Filter<MongoJob> = {
      active: true,
      nextRunAt: { $lte: now },
      $or: [{ lockedUntil: null }, { lockedUntil: { $lte: now } }],
    };
    const doc = await this.requireCollection().findOneAndUpdate(
      filter,
      {
        $set: {
          lockedBy: workerId,
          lockedUntil: now + lockMs,
          lockedAt: now,
          lastStartedAt: now,
          updatedAt: now,
        },
      },
      { sort: { nextRunAt: 1 }, returnDocument: "after" },
    );
    return doc ? fromMongo(doc) : null;
  }

  async heartbeat(
    id: JobId,
    workerId: string,
    lockMs: number,
  ): Promise<boolean> {
    const result = await this.requireCollection().updateOne(
      { _id: id, lockedBy: workerId },
      { $set: { lockedUntil: Date.now() + lockMs } },
    );
    return result.matchedCount === 1;
  }

  async releaseLock(
    id: JobId,
    workerId: string,
    patch: Partial<IJob>,
  ): Promise<boolean> {
    const result = await this.requireCollection().updateOne(
      { _id: id, lockedBy: workerId },
      {
        $set: {
          ...stripId(patch),
          lockedBy: null,
          lockedUntil: null,
          lockedAt: null,
          updatedAt: Date.now(),
        },
      },
    );
    return result.matchedCount === 1;
  }
}
