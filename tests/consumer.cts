import checkpoints = require('@lp-labs/distributed-checkpoints');

const store = checkpoints.createDistributedCheckpointStore(
  checkpoints.createMemoryDistributedCheckpointAdapter(),
  { ownerId: 'worker' },
);
void store.claim('key');
