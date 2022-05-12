﻿using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	// This makes sure we dont accidentally start trying to nest transactions or begin them concurrently
	// and facilities commiting the open transaction with a checkpoint
	public class TransactionManager<TTransaction> : ITransactionManager {
		private readonly ITransactionFactory<TTransaction> _factory;
		private readonly IScavengeMap<Unit, ScavengeCheckpoint> _storage;
		private bool _began;
		private TTransaction _transaction;

		public TransactionManager(
			ITransactionFactory<TTransaction> factory,
			IScavengeMap<Unit, ScavengeCheckpoint> storage) {

			_factory = factory;
			_storage = storage;
		}

		public void Begin() {
			if (_began)
				throw new InvalidOperationException("Cannot begin a transaction that has already begun.");

			//qq this may be enough already, but it may also be useful to start storing up
			// the updates in memory ourselves and write them when we complete the batch
			// if so, do this later.
			_transaction = _factory.Begin();
			_began = true;
		}

		public void Rollback() {
			if (!_began)
				throw new InvalidOperationException("Cannot rollback a transaction that has not begun.");

			_factory.Rollback(_transaction);
			_began = false;
		}

		public void Commit(ScavengeCheckpoint checkpoint) {
			if (!_began)
				throw new InvalidOperationException("Cannot commit a transaction that has not begun.");

			_storage[Unit.Instance] = checkpoint;

			//qqqq if we crash while commiting, will it get rolled back properly
			_factory.Commit(_transaction);
			_began = false;
		}
	}
}