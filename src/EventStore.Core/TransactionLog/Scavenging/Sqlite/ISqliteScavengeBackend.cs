namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	//qq consider renameing to IInitialize or something like that
	public interface ISqliteScavengeBackend {
		void Initialize(SqliteBackend sqlite);
	}
}
