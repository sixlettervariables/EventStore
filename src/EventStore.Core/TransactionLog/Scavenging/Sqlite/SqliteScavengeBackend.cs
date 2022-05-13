using System;
using System.IO;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteScavengeBackend<TStreamId> : ITransactionFactory<SqliteTransaction>, IDisposable {
		private const string DbFileName = "scavenging.db";
		private const string ExpectedJournalMode = "wal";
		private const int ExpectedSynchronousValue = 1; // Normal
		private SqliteConnection _connection;
		private SqliteBackend _sqliteBackend;

		public IScavengeMap<TStreamId, Unit> CollisionStorage { get; private set; }
		public IScavengeMap<ulong,TStreamId> Hashes { get; private set; }
		public IScavengeMap<ulong,DiscardPoint> MetaStorage { get; private set; }
		public IScavengeMap<TStreamId,DiscardPoint> MetaCollisionStorage { get; private set; }
		public IOriginalStreamScavengeMap<ulong> OriginalStorage { get; private set; }
		public IOriginalStreamScavengeMap<TStreamId> OriginalCollisionStorage { get; private set; }
		public IScavengeMap<Unit,ScavengeCheckpoint> CheckpointStorage { get; private set; }
		public IScavengeMap<int,ChunkTimeStampRange> ChunkTimeStampRanges { get; private set; }
		public IChunkWeightScavengeMap ChunkWeights { get; private set; }
		private ISqliteScavengeBackend[] AllMaps { get; set; }

		public void Initialize(string dir = ".") {
			OpenDbConnection(dir);
			ConfigureFeatures();

			var collisionStorage = new SqliteFixedStructScavengeMap<TStreamId, Unit>("CollisionStorageMap");
			CollisionStorage = collisionStorage;

			var hashes = new SqliteScavengeMap<ulong, TStreamId>("HashesMap");
			Hashes = hashes;

			var metaStorage = new SqliteFixedStructScavengeMap<ulong, DiscardPoint>("MetaStorageMap");
			MetaStorage = metaStorage;
			
			var metaCollisionStorage = new SqliteFixedStructScavengeMap<TStreamId, DiscardPoint>("MetaCollisionMap");
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

			AllMaps = new ISqliteScavengeBackend[] { collisionStorage, hashes, metaStorage, metaCollisionStorage,
				originalStorage, originalCollisionStorage, checkpointStorage, chunkTimeStampRanges, chunkWeights };

			var transaction = Begin();
			
			foreach (var map in AllMaps) {
				map.Initialize(_sqliteBackend);
			}
			
			Commit(transaction);
		}

		private void OpenDbConnection(string dir) {
			Directory.CreateDirectory(dir);

			var connectionStringBuilder = new SqliteConnectionStringBuilder();
			connectionStringBuilder.DataSource = Path.Combine(dir, DbFileName);
			_connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
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

		public SqliteTransaction Begin() {
			if (_connection == null) {
				throw new InvalidOperationException("Cannot start a scavenge state transaction without an open connection");
			}

			return _sqliteBackend.BeginTransaction();
		}

		public void Rollback(SqliteTransaction transaction) {
			if (transaction == null) {
				throw new InvalidOperationException("Cannot rollback a scavenge state transaction without an active transaction");
			}

			transaction.Rollback();
			transaction.Dispose();
			_sqliteBackend.ClearTransaction();
		}

		public void Commit(SqliteTransaction transaction) {
			if (transaction == null) {
				throw new InvalidOperationException("Cannot commit a scavenge state transaction without an active transaction");
			}
			
			transaction.Commit();
			transaction.Dispose();
			_sqliteBackend.ClearTransaction();
		}

		public void Dispose() {
			_connection?.Dispose();
		}
	}
}
