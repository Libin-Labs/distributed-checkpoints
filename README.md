# @lp-labs/distributed-checkpoints

Epoch-owned JSON checkpoint snapshots with an atomic adapter boundary. The package defines the
ownership protocol; only a backend that implements the contract atomically can coordinate separate
processes.

## Install and quick start

```sh
npm install @lp-labs/distributed-checkpoints
```

```js
import {
  createDistributedCheckpointStore,
  createMemoryDistributedCheckpointAdapter,
} from '@lp-labs/distributed-checkpoints';

// Process-local demonstration only; inject a durable atomic backend for distributed use.
const store = createDistributedCheckpointStore(createMemoryDistributedCheckpointAdapter(), {
  ownerId: 'worker-17',
});
const handle = await store.claim('job:42');
if (handle) {
  await handle.save({ step: 3, completed: ['download', 'index'] });
  await handle.release();
}
console.log((await store.load('job:42'))?.state);
```

## API, types, and customization

- `createDistributedCheckpointStore(adapter, { ownerId, clock?, limits? })` creates a store bound
  to one stable owner identity.
- `store.claim<T>(key)` atomically requests ownership and returns `null` on contention. A handle
  exposes `key`, `owner`, monotonic `epoch`, current `revision`, `active`, `save(state)`, and
  `release()`.
- `store.load<T>(key)` returns the latest detached checkpoint or `null`.
- `createMemoryDistributedCheckpointAdapter({ limits? })` supplies an atomic-in-this-process
  implementation for tests and single-process use. If raising the store state limits, configure
  the memory adapter with matching limits as well.
- Types: `CheckpointValue`, `CheckpointOwner`, `CheckpointRecord`, `DistributedCheckpointAdapter`,
  `CheckpointLimits`, `DistributedCheckpointOptions`, `CheckpointHandle`, and
  `DistributedCheckpointStore`. `DistributedCheckpointError` has `invalid`, `adapter`, and `state`
  codes.

State is copied as finite JSON; cycles, accessors, symbols, class instances, and non-finite numbers
are rejected. Defaults bound state to 1 MiB, depth 64, and 100,000 nodes; `limits` can lower or raise
these bounds. Keys and owner IDs are 1–256 characters.

## Adapter contract, interoperability, and limitations

`claim(key, owner)` must atomically return no owner while a claim is held, and otherwise allocate a
strictly increasing safe-integer epoch that is never reused across release, restart, or failover.
`save(owner, state, savedAt)` must atomically verify the exact key/owner/epoch/revision and persist
the next revision or return `null`. `release(owner)` must conditionally release only that exact
claim; `load(key)` returns a detached `{ key, owner, epoch, revision, state, savedAt }` snapshot.
Adapter methods may return values or promises.

The shared checkpoint shape (`key`, `owner`, `epoch`, `revision`, JSON `state`, `savedAt`) is
designed for application adapters and can be mapped to task identifiers/state used by
`@lp-labs/task-checkpoint`; neither package imports the other. The memory adapter loses all state
when its process exits and makes no distributed guarantee. This package provides no storage,
heartbeats, expiry, task runner, encryption, or exactly-once execution. A successful claim only
protects work if the backend enforces epochs atomically and downstream writes reject stale epochs.

ESM, CommonJS, and TypeScript declarations are included. CommonJS:
`const { createDistributedCheckpointStore } = require('@lp-labs/distributed-checkpoints')`.
There are no runtime or peer dependencies; TypeScript and Prettier are development-only.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
