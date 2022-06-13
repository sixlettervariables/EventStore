using System;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteScavengeBackend<TStreamId> {
		// WAL with SYNCHRONOUS NORMAL means that
		//  - commiting a transaction does not wait to it to flush to disk
		//  - which is nice and quick, but means in powerloss the last x transactions
		//    can be lost. the database will be in a valid state.
		//  - this is suitable for us because scavenge will continue from the last
		//    persisted checkpoint.
		private const string SqliteWalJournalMode = "wal";
		private const int SqliteNormalSynchronousValue = 1;
		private const int DefaultSqliteCacheSize = 2 * 1024 * 1024;
		private const int SqlitePageSize = 16 * 1024;
		private readonly int _cacheSizeInBytes;
		private SqliteBackend _sqliteBackend;

		private const int SchemaVersion = 1;
		
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

		public SqliteScavengeBackend(int cacheSizeInBytes = DefaultSqliteCacheSize) {
			_cacheSizeInBytes = Math.Abs(cacheSizeInBytes);
		}

		public void Initialize(SqliteConnection connection) {
			_sqliteBackend = new SqliteBackend(connection);
			
			ConfigureFeatures();
			InitializeSchemaVersion();

			var collisionStorage = new SqliteCollisionScavengeMap<TStreamId>();
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
			
			var chunkTimeStampRanges = new SqliteChunkTimeStampRangeScavengeMap();
			ChunkTimeStampRanges = chunkTimeStampRanges;
			
			var chunkWeights = new SqliteChunkWeightScavengeMap();
			ChunkWeights = chunkWeights;

			var transactionFactory = new SqliteTransactionFactory();
			TransactionFactory = transactionFactory;

			var allMaps = new IInitializeSqliteBackend[] { collisionStorage, hashes, metaStorage, metaCollisionStorage,
				originalStorage, originalCollisionStorage, checkpointStorage, chunkTimeStampRanges, chunkWeights,
				transactionFactory};

			foreach (var map in allMaps) {
				map.Initialize(_sqliteBackend);
			}
		}

		private void ConfigureFeatures() {
			_sqliteBackend.SetPragmaValue(SqliteBackend.PageSize, SqlitePageSize.ToString());
			var pageSize = int.Parse(_sqliteBackend.GetPragmaValue(SqliteBackend.PageSize));
			if (pageSize != SqlitePageSize) {
				// note we will fail to set it if the database already exists with a different page size
				throw new Exception($"Failed to configure page size to {SqlitePageSize}. Actually {pageSize}");
			}
			
			_sqliteBackend.SetPragmaValue(SqliteBackend.JournalMode, SqliteWalJournalMode);
			var journalMode = _sqliteBackend.GetPragmaValue(SqliteBackend.JournalMode);
			if (journalMode.ToLower() != SqliteWalJournalMode) {
				throw new Exception($"Failed to configure journal mode, unexpected value: {journalMode}");
			}
			
			_sqliteBackend.SetPragmaValue(SqliteBackend.Synchronous, SqliteNormalSynchronousValue.ToString());
			var synchronousMode = int.Parse(_sqliteBackend.GetPragmaValue(SqliteBackend.Synchronous));
			if (synchronousMode != SqliteNormalSynchronousValue) {
				throw new Exception($"Failed to configure synchronous mode, unexpected value: {synchronousMode}");
			}

			// cache size in kibi bytes is passed as a negative value, otherwise it's amount of pages
			var cacheSize = -1 * GetCacheSizeInKibiBytes();
			_sqliteBackend.SetPragmaValue(SqliteBackend.CacheSize, cacheSize.ToString());
			var currentCacheSize = int.Parse(_sqliteBackend.GetPragmaValue(SqliteBackend.CacheSize));
			if (currentCacheSize != cacheSize) {
				throw new Exception($"Failed to configure cache size, unexpected value: {currentCacheSize}");
			}
		}

		private int GetCacheSizeInKibiBytes() {
			var cacheSizeInKibiBytes = _cacheSizeInBytes / 1024;
			var defaultCacheSizeInKibiBytes = DefaultSqliteCacheSize / 1024;
			return Math.Max(cacheSizeInKibiBytes, defaultCacheSizeInKibiBytes);
		}

		private void InitializeSchemaVersion() {
			using (var cmd = _sqliteBackend.CreateCommand()) {
				cmd.CommandText = "CREATE TABLE IF NOT EXISTS ScavengingSchemaVersion (version Integer PRIMARY KEY)";
				cmd.ExecuteNonQuery();

				cmd.CommandText = "SELECT MAX(version) FROM ScavengingSchemaVersion";
				var currentVersion = cmd.ExecuteScalar();

				if (currentVersion == DBNull.Value) {
					cmd.CommandText = $"INSERT INTO ScavengingSchemaVersion VALUES({SchemaVersion})";
					cmd.ExecuteNonQuery();
				} else if (currentVersion != null && (long)currentVersion < SchemaVersion) {
					// need schema update
				}
			}
		}

		public SqliteBackend.Stats GetStats() {
			return _sqliteBackend.GetStats();
		}
	}
}
