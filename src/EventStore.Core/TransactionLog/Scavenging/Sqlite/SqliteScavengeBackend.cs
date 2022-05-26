using System;
using System.IO;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	//qq i suspect this needn't be the transactionfactory any more
	public class SqliteScavengeBackend<TStreamId> {
		private const string ExpectedJournalMode = "wal";
		private const int ExpectedSynchronousValue = 1; // Normal
		private SqliteConnection _connection;
		private SqliteBackend _sqliteBackend;

		public IScavengeMap<TStreamId, Unit> CollisionStorage { get; private set; }
		public IScavengeMap<ulong,TStreamId> Hashes { get; private set; }
		public IMetastreamScavengeMap<ulong> MetaStorage { get; private set; }
		public IMetastreamScavengeMap<TStreamId> MetaCollisionStorage { get; private set; }
		public IOriginalStreamScavengeMap<ulong> OriginalStorage { get; private set; }
		public IOriginalStreamScavengeMap<TStreamId> OriginalCollisionStorage { get; private set; }
		public IScavengeMap<Unit,ScavengeCheckpoint> CheckpointStorage { get; private set; }
		public IScavengeMap<int,ChunkTimeStampRange> ChunkTimeStampRanges { get; private set; }
		public IChunkWeightScavengeMap ChunkWeights { get; private set; }
		public ITransactionFactory<SqliteTransaction> TransactionFactory { get; private set; }

		public void Initialize(SqliteConnection connection) {
			OpenDbConnection(connection);
			ConfigureFeatures();

			var collisionStorage = new SqliteFixedStructScavengeMap<TStreamId, Unit>("CollisionStorageMap");
			CollisionStorage = collisionStorage;

			var hashes = new SqliteScavengeMap<ulong, TStreamId>("HashesMap");
			Hashes = hashes;

			var metaStorage = new SqliteMetastreamScavengeMap<ulong>("MetaStorageMap");
			MetaStorage = metaStorage;
			
			var metaCollisionStorage = new SqliteMetastreamScavengeMap<TStreamId>("MetaCollisionMap");
			MetaCollisionStorage = metaCollisionStorage;
			
			var originalStorage = new SqliteOriginalStreamScavengeMap<ulong>("OriginalStreamStorageMap");
			OriginalStorage = originalStorage;
			
			var originalCollisionStorage = new SqliteOriginalStreamScavengeMap<TStreamId>("OriginalStreamCollisionStorageMap");
			OriginalCollisionStorage = originalCollisionStorage;
			
			var checkpointStorage = new SqliteScavengeCheckpointMap<TStreamId>();
			CheckpointStorage = checkpointStorage;
			
			var chunkTimeStampRanges = new SqliteFixedStructScavengeMap<int, ChunkTimeStampRange>("ChunkTimeStampRangeMap");
			ChunkTimeStampRanges = chunkTimeStampRanges;
			
			var chunkWeights = new SqliteChunkWeightScavengeMap();
			ChunkWeights = chunkWeights;

			var transactionFactory = new SqliteTransactionFactory();
			TransactionFactory = transactionFactory;

			var allMaps = new ISqliteScavengeBackend[] { collisionStorage, hashes, metaStorage, metaCollisionStorage,
				originalStorage, originalCollisionStorage, checkpointStorage, chunkTimeStampRanges, chunkWeights,
				transactionFactory};

			foreach (var map in allMaps) {
				map.Initialize(_sqliteBackend);
			}
		}

		private void OpenDbConnection(SqliteConnection connection) {
			_connection = connection;
			_connection.Open();
			_sqliteBackend = new SqliteBackend(_connection);
		}

		private void ConfigureFeatures() {
			var cmd = _connection.CreateCommand();
			cmd.CommandText = $"PRAGMA journal_mode={ExpectedJournalMode}";
			cmd.ExecuteNonQuery();

			cmd.CommandText = "SELECT * FROM pragma_journal_mode()";
			var journalMode = cmd.ExecuteScalar();
			if (journalMode == null || journalMode.ToString().ToLower() != ExpectedJournalMode) {
				throw new Exception($"SQLite database is in unexpected journal mode: {journalMode}");
			}
			
			cmd.CommandText = $"PRAGMA synchronous={ExpectedSynchronousValue}";
			cmd.ExecuteNonQuery();
			
			cmd.CommandText = "SELECT * FROM pragma_synchronous()";
			var synchronousMode = (long?)cmd.ExecuteScalar();
			if (!synchronousMode.HasValue || synchronousMode.Value != ExpectedSynchronousValue) {
				throw new Exception($"SQLite database is in unexpected synchronous mode: {synchronousMode}");
			}
		}
	}
}
