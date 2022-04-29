namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryTransactionBackend : ITransactionBackend {
		public InMemoryTransactionBackend() {
		}

		public void Begin() {
			// sqlite implementation would open a transaction
		}

		public void Commit() {
			// sqlite implementation would commit the transaction
		}

		public void Rollback() {
			// sqlite implementation would roll back the transaction
		}
	}
}
