using System.Threading;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class OldScavengeChunkMergerBackend : IChunkMergerBackend {
		private readonly TFChunkDb _db;
		private readonly bool _unsafeIgnoreHardDeletes;

		public OldScavengeChunkMergerBackend(
			TFChunkDb db,
			bool unsafeIgnoreHardDeletes) {

			_db = db;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
		}

		public void MergeChunks(ITFChunkScavengerLog scavengerLogger, CancellationToken cancellationToken) {
			// todo: if time permits we could look in more detail at this implementation and see if it
			// could be improved or replaced.
			// todo: if time permits we could stop after the chunk with the scavenge point
			// todo: if time permits we could start with the minimum executed chunk this scavenge
			// todo: if time permits we could add some way of checkpointing during the merges
			//qq for now at least take a look and see how it works
			TFChunkScavenger.MergePhase(
				db: _db,
				maxChunkDataSize: _db.Config.ChunkSize,
				scavengerLog: scavengerLogger,
				unsafeIgnoreHardDeletes: _unsafeIgnoreHardDeletes,
				ct: cancellationToken);
		}
	}
}
