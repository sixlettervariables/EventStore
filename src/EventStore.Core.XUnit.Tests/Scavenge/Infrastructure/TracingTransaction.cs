using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TracingTransaction : ITransaction {
		private readonly ITransaction _wrapped;
		private readonly Tracer _tracer;

		public TracingTransaction(ITransaction wrapped, Tracer tracer) {
			_wrapped = wrapped;
			_tracer = tracer;
		}

		public void Begin() {
			_wrapped.Begin();
		}

		public void Commit(ScavengeCheckpoint checkpoint) {
			_tracer.Trace($"Checkpoint: {checkpoint}");
			_wrapped.Commit(checkpoint);
		}

		public void Rollback() {
			_wrapped.Rollback();
		}
	}
}
