namespace EventStore.Core.TransactionLog.Scavenging {
	public class IndexReaderForAccumulator<TStreamId> : IIndexReaderForAccumulator<TStreamId> {
		public bool HashInUseBefore(ulong hash, long postion, out TStreamId hashUser) {
			throw new System.NotImplementedException();
		}
	}
}
