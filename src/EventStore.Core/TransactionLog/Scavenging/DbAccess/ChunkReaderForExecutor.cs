using System;
using System.Collections.Generic;
using EventStore.Core.TransactionLog.Chunks.TFChunk;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq implement
	public class ChunkReaderForExecutor : IChunkReaderForExecutor<string> {
		private readonly TFChunk _chunk;

		public ChunkReaderForExecutor(TFChunk chunk) {
			_chunk = chunk;
		}

		public int ChunkStartNumber => _chunk.ChunkHeader.ChunkStartNumber;

		public int ChunkEndNumber => _chunk.ChunkHeader.ChunkEndNumber;

		public bool IsReadOnly => _chunk.IsReadOnly;

		public long ChunkEndPosition => _chunk.ChunkHeader.ChunkEndPosition;

		public IEnumerable<RecordForScavenge<string>> ReadRecords() {
			yield return new RecordForScavenge<string>() {
				StreamId = "thestream",
				TimeStamp = DateTime.UtcNow,
				EventNumber = 123,
				RecordBytes = null,
			};
		}
	}
}
