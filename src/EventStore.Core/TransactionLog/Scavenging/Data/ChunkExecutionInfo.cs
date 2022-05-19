using System;

//qq own files for some stuff in here
namespace EventStore.Core.TransactionLog.Scavenging {
	// For ChunkExecutor, which implements maxAge more accurately than the index executor
	public struct ChunkExecutionInfo {
		public ChunkExecutionInfo(
			bool isTombstoned,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint,
			TimeSpan? maxAge) {

			IsTombstoned = isTombstoned;
			DiscardPoint = discardPoint;
			MaybeDiscardPoint = maybeDiscardPoint;
			MaxAge = maxAge;
		}

		public bool IsTombstoned { get; }
		public DiscardPoint DiscardPoint { get; }
		public DiscardPoint MaybeDiscardPoint { get; }
		public TimeSpan? MaxAge { get; }
	}
}
