using System;
using System.Collections.Generic;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TracingChunkReaderForAccumulator<TStreamId> : IChunkReaderForAccumulator<TStreamId> {
		private readonly IChunkReaderForAccumulator<TStreamId> _wrapped;
		private readonly Action<string> _trace;

		public TracingChunkReaderForAccumulator(
			IChunkReaderForAccumulator<TStreamId> wrapped,
			Action<string> trace) {

			_wrapped = wrapped;
			_trace = trace;
		}

		public IEnumerable<RecordForAccumulator<TStreamId>> ReadChunk(int logicalChunkNumber) {
			var ret = _wrapped.ReadChunk(logicalChunkNumber);
			_trace($"Reading Chunk {logicalChunkNumber}");
			return ret;
		}
	}
}
