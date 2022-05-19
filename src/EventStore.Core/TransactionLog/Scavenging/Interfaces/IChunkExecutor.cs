using System.Threading;

namespace EventStore.Core.TransactionLog.Scavenging {
	// The chunk executor performs the actual removal of the log records
	public interface IChunkExecutor<TStreamId> {
		void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken);

		void Execute(
			ScavengeCheckpoint.ExecutingChunks checkpoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken);
	}
}
