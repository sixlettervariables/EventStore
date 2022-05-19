using System;
using System.Threading;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Cleaner : ICleaner {
		private readonly bool _unsafeIgnoreHardDeletes;

		public Cleaner(
			bool unsafeIgnoreHardDeletes) {
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
		}

		public void Clean(
			ScavengePoint scavengePoint,
			IScavengeStateForCleaner state,
			CancellationToken cancellationToken) {

			var checkpoint = new ScavengeCheckpoint.Cleaning(scavengePoint);
			state.SetCheckpoint(checkpoint);
			Clean(checkpoint, state, cancellationToken);
		}

		public void Clean(
			ScavengeCheckpoint.Cleaning checkpoint,
			IScavengeStateForCleaner state,
			CancellationToken cancellationToken) {

			cancellationToken.ThrowIfCancellationRequested();

			// we clean up in a transaction, not so that we can checkpoint, but just to save lots of
			// implicit transactions from being created
			var transaction = state.BeginTransaction();
			try {
				CleanImpl(state, cancellationToken);
				transaction.Commit(checkpoint);
			} catch {
				transaction.Rollback();
				throw;
			}
		}

		private void CleanImpl(
			IScavengeStateForCleaner state,
			CancellationToken cancellationToken) {

			// constant time operation
			if (state.AllChunksExecuted()) {
				// Now we know we have successfully executed every chunk with weight.

				state.DeleteMetastreamData();

				cancellationToken.ThrowIfCancellationRequested();

				state.DeleteOriginalStreamData(deleteArchived: _unsafeIgnoreHardDeletes);

			} else {
				// one or more chunks was not executed, due to error or not meeting the threshold
				// either way, we cannot remove records 
				if (_unsafeIgnoreHardDeletes) {
					//qq this is a pretty serious condition, we should probably make it impossible.
					// we could have removed the tombstone without removing all the other records.

				} else {
					//qq this is fine just log something about not cleaning the state because there are
					// chunks pending execution.
				}
			}
		}
	}
}
