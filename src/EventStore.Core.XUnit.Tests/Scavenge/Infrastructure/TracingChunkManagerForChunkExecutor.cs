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

		public bool TrySwitchChunk(
			TChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName) {

			var ret = _wrapped.TrySwitchChunk(
				chunk,
				verifyHash,
				removeChunksWithGreaterNumbers,
				out newFileName);

			if (ret) {
				_tracer.Trace($"Switched in chunk {newFileName}");
			} else {
				_tracer.Trace($"Did not switch in chunk {newFileName}");
			}

			return ret;
		}
	}
}
