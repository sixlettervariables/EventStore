using System;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq implement
	public class ChunkManagerForScavenge : IChunkManagerForChunkExecutor<string, TFChunk> {
		private readonly TFChunkManager _manager;
		private readonly TFChunkDbConfig _dbConfig;

		public ChunkManagerForScavenge(TFChunkManager manager, TFChunkDbConfig dbConfig) {
			_manager = manager;
			_dbConfig = dbConfig;
		}

		public IChunkWriterForExecutor<string, TFChunk> CreateChunkWriter(
			int chunkStartNumber, //qq pass along
			int chunkEndNumber) {

			return new ChunkWriterForExecutor(_dbConfig);
		}

		public IChunkReaderForExecutor<string> GetChunkReaderFor(long position) {
			var tfChunk = _manager.GetChunkFor(position);
			return new ChunkReaderForExecutor(tfChunk);
		}

		public void SwitchChunk(
			TFChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName) {

			var tfChunk = _manager.SwitchChunk(
				chunk,
				verifyHash,
				removeChunksWithGreaterNumbers);

			if (tfChunk == null) {
				throw new Exception(); //qq detail, old scavenge handles this but it looks like its impossible
			}

			newFileName = tfChunk.FileName;
		}
	}
}
