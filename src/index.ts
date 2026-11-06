export type CheckpointValue =
  null | boolean | number | string | CheckpointValue[] | { [key: string]: CheckpointValue };

export interface CheckpointOwner {
  readonly key: string;
  readonly owner: string;
  readonly epoch: number;
  readonly revision: number;
}

export interface CheckpointRecord<T extends CheckpointValue = CheckpointValue> {
  readonly key: string;
  readonly owner: string;
  readonly epoch: number;
  readonly revision: number;
  readonly state: T;
  readonly savedAt: number;
}

export interface DistributedCheckpointAdapter {
  claim(key: string, owner: string): PromiseLike<CheckpointOwner | null> | CheckpointOwner | null;
  load(key: string): PromiseLike<CheckpointRecord | null> | CheckpointRecord | null;
  save(
    owner: CheckpointOwner,
    state: CheckpointValue,
    savedAt: number,
  ): PromiseLike<CheckpointRecord | null> | CheckpointRecord | null;
  release(owner: CheckpointOwner): PromiseLike<boolean> | boolean;
}

export interface CheckpointLimits {
  readonly maxBytes?: number;
  readonly maxDepth?: number;
  readonly maxNodes?: number;
}

export interface DistributedCheckpointOptions {
  readonly ownerId: string;
  readonly clock?: () => number;
  readonly limits?: CheckpointLimits;
}

export interface CheckpointHandle<T extends CheckpointValue = CheckpointValue> {
  readonly key: string;
  readonly owner: string;
  readonly epoch: number;
  readonly revision: number;
  readonly active: boolean;
  save(state: T): Promise<CheckpointRecord<T>>;
  release(): Promise<boolean>;
}

export interface DistributedCheckpointStore {
  claim<T extends CheckpointValue = CheckpointValue>(
    key: string,
  ): Promise<CheckpointHandle<T> | null>;
  load<T extends CheckpointValue = CheckpointValue>(
    key: string,
  ): Promise<CheckpointRecord<T> | null>;
}

export class DistributedCheckpointError extends Error {
  constructor(
    readonly code: 'invalid' | 'adapter' | 'state',
    message: string,
  ) {
    super(message);
    this.name = 'DistributedCheckpointError';
  }
}

const defaults = { maxBytes: 1_048_576, maxDepth: 64, maxNodes: 100_000 };

function configuredLimits(input: CheckpointLimits = {}) {
  const limits = {
    maxBytes: input.maxBytes ?? defaults.maxBytes,
    maxDepth: input.maxDepth ?? defaults.maxDepth,
    maxNodes: input.maxNodes ?? defaults.maxNodes,
  };
  if (
    !Number.isSafeInteger(limits.maxBytes) ||
    limits.maxBytes < 1 ||
    !Number.isSafeInteger(limits.maxDepth) ||
    limits.maxDepth < 0 ||
    !Number.isSafeInteger(limits.maxNodes) ||
    limits.maxNodes < 1
  )
    throw new DistributedCheckpointError('invalid', 'Checkpoint limits are outside valid bounds.');
  return limits;
}

function cloneState(value: unknown, limits: ReturnType<typeof configuredLimits>): CheckpointValue {
  const seen = new Set<object>();
  let nodes = 0;
  const visit = (item: unknown, depth: number): CheckpointValue => {
    if (++nodes > limits.maxNodes || depth > limits.maxDepth)
      throw new DistributedCheckpointError('state', 'Checkpoint state exceeds structural limits.');
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return item;
    if (typeof item === 'number' && Number.isFinite(item)) return item;
    if (Array.isArray(item)) {
      if (seen.has(item))
        throw new DistributedCheckpointError('state', 'Cycles are not supported.');
      seen.add(item);
      const copy = Array.from(item, (child) => visit(child, depth + 1));
      seen.delete(item);
      return copy;
    }
    if (typeof item === 'object' && Object.getPrototypeOf(item) === Object.prototype) {
      if (seen.has(item))
        throw new DistributedCheckpointError('state', 'Cycles are not supported.');
      if (Object.getOwnPropertySymbols(item).length)
        throw new DistributedCheckpointError('state', 'Symbol keys are not supported.');
      seen.add(item);
      const copy: Record<string, CheckpointValue> = {};
      for (const key of Object.keys(item).sort()) {
        const descriptor = Object.getOwnPropertyDescriptor(item, key);
        if (!descriptor || !('value' in descriptor))
          throw new DistributedCheckpointError('state', 'Accessor properties are not supported.');
        Object.defineProperty(copy, key, {
          value: visit(descriptor.value, depth + 1),
          enumerable: true,
          configurable: true,
          writable: true,
        });
      }
      seen.delete(item);
      return copy;
    }
    throw new DistributedCheckpointError('state', 'State must contain only finite JSON values.');
  };
  const copy = visit(value, 0);
  if (new TextEncoder().encode(JSON.stringify(copy)).byteLength > limits.maxBytes)
    throw new DistributedCheckpointError('state', 'Checkpoint state exceeds the byte limit.');
  return copy;
}

function validKey(key: string): void {
  if (typeof key !== 'string' || key.length < 1 || key.length > 256)
    throw new DistributedCheckpointError('invalid', 'key must contain 1..256 characters.');
}

function validOwner(value: unknown): value is CheckpointOwner {
  if (!value || typeof value !== 'object') return false;
  const owner = value as CheckpointOwner;
  return (
    typeof owner.key === 'string' &&
    typeof owner.owner === 'string' &&
    Number.isSafeInteger(owner.epoch) &&
    owner.epoch > 0 &&
    Number.isSafeInteger(owner.revision) &&
    owner.revision >= 0
  );
}

function validRecord(value: unknown): value is CheckpointRecord {
  if (!value || typeof value !== 'object') return false;
  const record = value as CheckpointRecord;
  return (
    validOwner(record) &&
    Number.isSafeInteger(record.savedAt) &&
    record.savedAt >= 0 &&
    Object.prototype.hasOwnProperty.call(record, 'state')
  );
}

export function createDistributedCheckpointStore(
  adapter: DistributedCheckpointAdapter,
  options: DistributedCheckpointOptions,
): DistributedCheckpointStore {
  if (
    !adapter ||
    typeof adapter.claim !== 'function' ||
    typeof adapter.load !== 'function' ||
    typeof adapter.save !== 'function' ||
    typeof adapter.release !== 'function'
  )
    throw new DistributedCheckpointError(
      'invalid',
      'Adapter must implement claim, load, save, release.',
    );
  if (
    !options ||
    typeof options.ownerId !== 'string' ||
    !options.ownerId.length ||
    options.ownerId.length > 256
  )
    throw new DistributedCheckpointError('invalid', 'ownerId must contain 1..256 characters.');
  const ownerId = options.ownerId;
  const clock = options.clock ?? Date.now;
  if (typeof clock !== 'function')
    throw new DistributedCheckpointError('invalid', 'clock must be a function.');
  const limits = configuredLimits(options.limits);
  return {
    async claim<T extends CheckpointValue>(key: string) {
      validKey(key);
      const owner = await adapter.claim(key, ownerId);
      if (owner === null) return null;
      if (!validOwner(owner) || owner.key !== key || owner.owner !== ownerId)
        throw new DistributedCheckpointError(
          'adapter',
          'Adapter returned an invalid checkpoint owner.',
        );
      let active = true;
      let revision = owner.revision;
      return {
        key,
        owner: ownerId,
        epoch: owner.epoch,
        get revision() {
          return revision;
        },
        get active() {
          return active;
        },
        async save(state: T) {
          if (!active)
            throw new DistributedCheckpointError('invalid', 'Checkpoint ownership is inactive.');
          const copy = cloneState(state, limits) as T;
          const savedAt = clock();
          if (!Number.isSafeInteger(savedAt) || savedAt < 0)
            throw new DistributedCheckpointError(
              'invalid',
              'clock must return a non-negative safe integer.',
            );
          const saved = await adapter.save(
            { key, owner: ownerId, epoch: owner.epoch, revision },
            copy,
            savedAt,
          );
          if (saved === null) {
            active = false;
            throw new DistributedCheckpointError(
              'adapter',
              'Checkpoint ownership was lost before save.',
            );
          }
          if (
            !validRecord(saved) ||
            saved.key !== key ||
            saved.owner !== ownerId ||
            saved.epoch !== owner.epoch ||
            saved.revision !== revision + 1
          )
            throw new DistributedCheckpointError(
              'adapter',
              'Adapter returned an invalid checkpoint record.',
            );
          revision = saved.revision;
          return { ...saved, state: cloneState(saved.state, limits) as T };
        },
        async release() {
          if (!active) return false;
          active = false;
          const released = await adapter.release({
            key,
            owner: ownerId,
            epoch: owner.epoch,
            revision,
          });
          if (typeof released !== 'boolean')
            throw new DistributedCheckpointError(
              'adapter',
              'Adapter release must return a boolean.',
            );
          return released;
        },
      };
    },
    async load<T extends CheckpointValue>(key: string) {
      validKey(key);
      const record = await adapter.load(key);
      if (record === null) return null;
      if (!validRecord(record) || record.key !== key)
        throw new DistributedCheckpointError(
          'adapter',
          'Adapter returned an invalid checkpoint record.',
        );
      return { ...record, state: cloneState(record.state, limits) as T };
    },
  };
}

export function createMemoryDistributedCheckpointAdapter(
  options: { readonly limits?: CheckpointLimits } = {},
): DistributedCheckpointAdapter {
  const limits = configuredLimits(options.limits);
  const epochs = new Map<string, number>();
  const owners = new Map<string, CheckpointOwner>();
  const records = new Map<string, CheckpointRecord>();
  return {
    claim(key, owner) {
      if (owners.has(key)) return null;
      const epoch = (epochs.get(key) ?? 0) + 1;
      if (!Number.isSafeInteger(epoch))
        throw new DistributedCheckpointError(
          'adapter',
          'Epoch counter exceeded safe integer bounds.',
        );
      epochs.set(key, epoch);
      const next = { key, owner, epoch, revision: 0 };
      owners.set(key, next);
      return { ...next };
    },
    load(key) {
      const record = records.get(key);
      return record ? { ...record, state: cloneState(record.state, limits) } : null;
    },
    save(owner, state, savedAt) {
      const current = owners.get(owner.key);
      if (
        !current ||
        current.owner !== owner.owner ||
        current.epoch !== owner.epoch ||
        current.revision !== owner.revision
      )
        return null;
      const stateCopy = cloneState(state, limits);
      const next = { ...current, revision: current.revision + 1 };
      if (!Number.isSafeInteger(next.revision))
        throw new DistributedCheckpointError(
          'adapter',
          'Revision counter exceeded safe integer bounds.',
        );
      const record = { ...next, savedAt, state: stateCopy };
      const returnedState = cloneState(stateCopy, limits);
      owners.set(owner.key, next);
      records.set(owner.key, record);
      return { ...record, state: returnedState };
    },
    release(owner) {
      const current = owners.get(owner.key);
      if (
        !current ||
        current.owner !== owner.owner ||
        current.epoch !== owner.epoch ||
        current.revision !== owner.revision
      )
        return false;
      owners.delete(owner.key);
      return true;
    },
  };
}
