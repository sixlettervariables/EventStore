using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IChunkReaderForExecutor<TStreamId> {
		int ChunkStartNumber { get; }
		int ChunkEndNumber { get; }
		bool IsReadOnly { get; }
		long ChunkEndPosition { get; }
		IEnumerable<RecordForScavenge<TStreamId>> ReadRecords();
	}
}
