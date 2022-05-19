namespace EventStore.Core.TransactionLog.Scavenging {
	public struct MetastreamData {
		public static MetastreamData Empty { get; } = new MetastreamData(
			isTombstoned: false,
			discardPoint: DiscardPoint.KeepAll);

		public MetastreamData(
			bool isTombstoned,
			DiscardPoint discardPoint) {

			IsTombstoned = isTombstoned;
			DiscardPoint = discardPoint;
		}

		/// <summary>
		/// True when the corresponding original stream is tombstoned
		/// </summary>
		public bool IsTombstoned { get; }

		public DiscardPoint DiscardPoint { get; }
	}
}
