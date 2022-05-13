namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public interface ISqliteScavengeBackend {
		void Initialize(SqliteBackend sqlite);
	}
}
