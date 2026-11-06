import {
  createDistributedCheckpointStore,
  createMemoryDistributedCheckpointAdapter,
  type CheckpointRecord,
} from '@lp-labs/distributed-checkpoints';

const store = createDistributedCheckpointStore(createMemoryDistributedCheckpointAdapter(), {
  ownerId: 'worker',
});
const record: Promise<CheckpointRecord | null> = store.load('key');
void record;
