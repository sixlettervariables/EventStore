namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IChunkWriterForExecutor<TStreamId, TChunk> {
		TChunk WrittenChunk { get; }

		void WriteRecord(RecordForScavenge<TStreamId> record);
		//qq finalize, etc.
	}
}
