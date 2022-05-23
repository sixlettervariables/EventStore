using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TracingChunkWriterForExecutor<TStreamId, TRecord> :
		IChunkWriterForExecutor<TStreamId, TRecord> {

		private readonly IChunkWriterForExecutor<TStreamId, TRecord> _wrapped;
		private readonly Tracer _tracer;

		public TracingChunkWriterForExecutor(
			IChunkWriterForExecutor<TStreamId, TRecord> wrapped,
			Tracer tracer) {

			_wrapped = wrapped;
			_tracer = tracer;
		}

		public void SwitchIn(out string newFileName) {
			_wrapped.SwitchIn(out newFileName);
			_tracer.Trace($"Switched in chunk {newFileName}");
		}

		public void WriteRecord(RecordForExecutor<TStreamId, TRecord> record) {
			_wrapped.WriteRecord(record);
		}
	}
}
