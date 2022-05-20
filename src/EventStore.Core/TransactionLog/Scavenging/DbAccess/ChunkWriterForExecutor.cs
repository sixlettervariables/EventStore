using System;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkWriterForExecutor : IChunkWriterForExecutor<string, TFChunk> {
		private readonly TFChunk _tfChunk;

		public ChunkWriterForExecutor(TFChunkDbConfig dbConfig) {
			_tfChunk = null;
			//qq _tfChunk = TFChunk.CreateNew(...)
		}

		public TFChunk WrittenChunk => _tfChunk;

		public void WriteRecord(RecordForScavenge<string> record) {
			//qq _tfChunk.TryAppend(...)
			throw new NotImplementedException();
		}
	}
}
