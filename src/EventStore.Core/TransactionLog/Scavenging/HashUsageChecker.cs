namespace EventStore.Core.TransactionLog.Scavenging {
	public class HashUsageChecker<TStreamId> : IHashUsageChecker<TStreamId> {
		public bool HashInUseBefore(ulong hash, long postion, out TStreamId hashUser) {
			throw new System.NotImplementedException();
		}
	}
}
