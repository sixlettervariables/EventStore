namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IChunkManagerForChunkExecutor<TStreamId, TChunk> {
		IChunkWriterForExecutor<TStreamId, TChunk> CreateChunkWriter(
			int chunkStartNumber,
			int chunkEndNumber);

		IChunkReaderForExecutor<TStreamId> GetChunkReaderFor(long position);

		void SwitchChunk(
			TChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName);
	}
}
