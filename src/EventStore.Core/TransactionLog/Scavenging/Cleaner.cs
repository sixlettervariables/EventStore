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

			if (_unsafeIgnoreHardDeletes) {
				var transaction = state.BeginTransaction();
				try {
					//qqq check cancellation sometimes
					state.DeleteTombstonedOriginalStreams();
					state.DeleteTombstonedMetastreams();

					transaction.Commit(checkpoint);
				} catch {
					transaction.Rollback();
					throw;
				}
			}

			//qq we could state.DeleteTombstonedMetastreams(); even if unsafeIgnoreHardDeletes is off
			// but we could only do that if we know that we've actually executed all the stuff we need to
			// which a threshold0 or perhaps threshold1 might be sufficient for.
			// and we could make such a threshold the default for grdpr reasons and cleanup when it is
			// set: if (checkpoint.ScavengePoint.Threshold <)
		}
	}
}
