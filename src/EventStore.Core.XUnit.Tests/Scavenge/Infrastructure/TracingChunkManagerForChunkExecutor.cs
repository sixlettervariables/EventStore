using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TracingChunkManagerForChunkExecutor<TStreamId, TChunk> :
		IChunkManagerForChunkExecutor<TStreamId, TChunk> {

		private readonly IChunkManagerForChunkExecutor<TStreamId, TChunk> _wrapped;
		private readonly Tracer _tracer;

		public TracingChunkManagerForChunkExecutor(IChunkManagerForChunkExecutor<TStreamId, TChunk> wrapped, Tracer tracer) {
			_wrapped = wrapped;
			_tracer = tracer;
		}

		public IChunkWriterForExecutor<TStreamId, TChunk> CreateChunkWriter(
			int chunkStartNumber,
			int chunkEndNumber) {

			return _wrapped.CreateChunkWriter(chunkStartNumber, chunkEndNumber);
		}

		public IChunkReaderForExecutor<TStreamId> GetChunkReaderFor(long position) {
			var ret = _wrapped.GetChunkReaderFor(position);
			_tracer.Trace($"Opening Chunk {ret.ChunkStartNumber}-{ret.ChunkEndNumber}");
			return ret;
		}

		public void SwitchChunk(
			TChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName) {

			_wrapped.SwitchChunk(
				chunk,
				verifyHash,
				removeChunksWithGreaterNumbers,
				out newFileName);

			_tracer.Trace($"Switched in chunk {newFileName}");
		}
	}
}
