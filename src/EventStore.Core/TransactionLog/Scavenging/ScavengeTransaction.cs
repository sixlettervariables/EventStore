using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq own file. name
	public class ScavengeTransaction : ITransaction {
		private readonly ITransactionBackend _backend;
		private readonly IScavengeMap<Unit, ScavengeCheckpoint> _storage;
		private bool _began;

		public ScavengeTransaction(
			ITransactionBackend backend,
			IScavengeMap<Unit, ScavengeCheckpoint> storage) {

			_backend = backend;
			_storage = storage;
		}

		public void Begin() {
			if (_began)
				throw new InvalidOperationException("Cannot begin a transaction that has already begun.");

			//qq this may be enough already, but it may also be useful to start storing up
			// the updates in memory ourselves and write them when we complete the batch
			// if so, do this later.
			_backend.Begin();
			_began = true;
		}

		public void Rollback() {
			if (!_began)
				throw new InvalidOperationException("Cannot rollback a transaction that has not begun.");

			_backend.Rollback();
			_began = false;
		}

		public void Commit(ScavengeCheckpoint checkpoint) {
			if (!_began)
				throw new InvalidOperationException("Cannot commit a transaction that has not begun.");

			_storage[Unit.Instance] = checkpoint;

			//qqqq if we crash while commiting, will it get rolled back properly
			_backend.Commit();
			_began = false;
		}
	}
}
