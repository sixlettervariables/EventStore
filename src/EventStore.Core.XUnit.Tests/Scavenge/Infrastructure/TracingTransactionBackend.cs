using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TracingTransactionBackend : ITransactionBackend {
		private readonly ITransactionBackend _wrapped;
		private readonly Tracer _tracer;

		public TracingTransactionBackend(ITransactionBackend wrapped, Tracer tracer) {
			_wrapped = wrapped;
			_tracer = tracer;
		}

		public void Begin() {
			_tracer.TraceIn("Begin");
			_wrapped.Begin();
		}

		public void Commit() {
			_wrapped.Commit();
			_tracer.TraceOut("Commit");
		}

		public void Rollback() {
			_wrapped.Rollback();
			_tracer.TraceOut("Rollback");
		}
	}
}
