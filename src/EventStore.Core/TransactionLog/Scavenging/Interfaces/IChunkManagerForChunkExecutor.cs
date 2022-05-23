using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IChunkManagerForChunkExecutor<TStreamId, TRecord> {
		IChunkWriterForExecutor<TStreamId, TRecord> CreateChunkWriter(
			IChunkReaderForExecutor<TStreamId, TRecord> sourceChunk);

		IChunkReaderForExecutor<TStreamId, TRecord> GetChunkReaderFor(long position);
	}

	public interface IChunkWriterForExecutor<TStreamId, TRecord> {
		void WriteRecord(RecordForExecutor<TStreamId, TRecord> record);

		void SwitchIn(out string newFileName);
	}

	public interface IChunkReaderForExecutor<TStreamId, TRecord> {
		int ChunkStartNumber { get; }
		int ChunkEndNumber { get; }
		bool IsReadOnly { get; }
		long ChunkEndPosition { get; }
		IEnumerable<bool> ReadInto(
			RecordForExecutor<TStreamId, TRecord>.NonPrepare nonPrepare,
			RecordForExecutor<TStreamId, TRecord>.Prepare prepare);
	}
}
