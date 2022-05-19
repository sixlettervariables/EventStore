//qq own files for some stuff in here
namespace EventStore.Core.TransactionLog.Scavenging {
	public struct IndexExecutionInfo {
		public IndexExecutionInfo(
			bool isMetastream,
			bool isTombstoned,
			DiscardPoint discardPoint) {

			IsMetastream = isMetastream;
			IsTombstoned = isTombstoned;
			DiscardPoint = discardPoint;
		}

		public bool IsMetastream { get; }

		/// <summary>
		/// True when the corresponding original stream is tombstoned
		/// </summary>
		public bool IsTombstoned { get; }

		public DiscardPoint DiscardPoint { get; }
	}
}
