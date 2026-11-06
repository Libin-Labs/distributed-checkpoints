import test from 'node:test';
import assert from 'node:assert/strict';
import {
  createDistributedCheckpointStore,
  createMemoryDistributedCheckpointAdapter,
  DistributedCheckpointError,
} from '../dist/esm/index.js';

test('claims are exclusive, epochs increase, and latest snapshots survive release', async () => {
  const adapter = createMemoryDistributedCheckpointAdapter();
  const firstStore = createDistributedCheckpointStore(adapter, {
    ownerId: 'worker-a',
    clock: () => 10,
  });
  const secondStore = createDistributedCheckpointStore(adapter, {
    ownerId: 'worker-b',
    clock: () => 20,
  });
  const first = await firstStore.claim('job');
  assert.ok(first);
  assert.equal(await secondStore.claim('job'), null);
  const saved = await first.save({ step: 3, completed: ['a'] });
  assert.equal(saved.epoch, first.epoch);
  assert.equal(saved.revision, 1);
  assert.equal(await first.release(), true);
  const next = await secondStore.claim('job');
  assert.ok(next);
  assert.ok(next.epoch > first.epoch);
  assert.deepEqual((await secondStore.load('job'))?.state, { completed: ['a'], step: 3 });
  await next.release();
});

test('snapshots are detached and updates after release are rejected', async () => {
  const store = createDistributedCheckpointStore(createMemoryDistributedCheckpointAdapter(), {
    ownerId: 'worker',
  });
  const handle = await store.claim('k');
  assert.ok(handle);
  const state = { nested: [1] };
  await handle.save(state);
  state.nested.push(2);
  const loaded = await store.load('k');
  assert.deepEqual(loaded?.state, { nested: [1] });
  if (loaded) loaded.state.nested.push(3);
  assert.deepEqual((await store.load('k'))?.state, { nested: [1] });
  await handle.release();
  await assert.rejects(handle.save({ stale: true }), { code: 'invalid' });
});

test('state limits and adapter contract failures are surfaced', async () => {
  const store = createDistributedCheckpointStore(createMemoryDistributedCheckpointAdapter(), {
    ownerId: 'worker',
    limits: { maxBytes: 8 },
  });
  const handle = await store.claim('k');
  assert.ok(handle);
  await assert.rejects(handle.save({ large: 'payload' }), DistributedCheckpointError);
  const invalid = createDistributedCheckpointStore(
    {
      claim: () => ({ key: 'other', owner: 'wrong', epoch: 0, revision: 0 }),
      load: () => null,
      save: () => null,
      release: () => false,
    },
    { ownerId: 'worker' },
  );
  await assert.rejects(invalid.claim('k'), { code: 'adapter' });
});

test('a rejected adapter save does not partially advance the in-memory revision', async () => {
  const adapter = createMemoryDistributedCheckpointAdapter({ limits: { maxBytes: 3 } });
  const store = createDistributedCheckpointStore(adapter, {
    ownerId: 'worker',
    limits: { maxBytes: 100 },
  });
  const handle = await store.claim('atomic');
  assert.ok(handle);
  await assert.rejects(handle.save({ too: 'large' }), { code: 'state' });
  assert.equal(handle.revision, 0);
  assert.equal((await handle.save(1)).revision, 1);
});
