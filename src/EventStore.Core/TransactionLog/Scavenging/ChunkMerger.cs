using System.Threading;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkMerger : IChunkMerger {
		private readonly bool _mergeChunks;
		private readonly IChunkMergerBackend _backend;

		public ChunkMerger(
			bool mergeChunks,
			IChunkMergerBackend backend) {

			_mergeChunks = mergeChunks;
			_backend = backend;
		}

		public void MergeChunks(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkMerger state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			var checkpoint = new ScavengeCheckpoint.MergingChunks(scavengePoint);
			state.SetCheckpoint(checkpoint);
			MergeChunks(checkpoint, state, scavengerLogger, cancellationToken);
		}

		public void MergeChunks(
			ScavengeCheckpoint.MergingChunks checkpoint,
			IScavengeStateForChunkMerger state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			if (_mergeChunks) {
				_backend.MergeChunks(scavengerLogger, cancellationToken);
			}
		}
	}
}
