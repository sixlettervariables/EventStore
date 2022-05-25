using System.Threading;
using EventStore.Common.Log;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkMerger : IChunkMerger {
		protected static readonly ILogger Log = LogManager.GetLoggerFor<ChunkMerger>();

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

			Log.Trace("Starting new scavenge chunk merging phase for {scavengePoint}",
				scavengePoint.GetName());

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
				Log.Trace("Merging chunks from checkpoint: {checkpoint}", checkpoint);
				_backend.MergeChunks(scavengerLogger, cancellationToken);
			} else {
				Log.Trace("Merging chunks is disabled");
			}
		}
	}
}
