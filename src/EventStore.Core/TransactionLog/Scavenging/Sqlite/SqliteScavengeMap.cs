using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;

// IScavengeMap<TStreamId, Unit> collisionStorage,                        # very small (probably empty). key and value are fixed length
// IScavengeMap<ulong, TStreamId> hashes,                                 # large - one entry per stream. key and value are fixed length
// IScavengeMap<ulong, MetastreamData> metaStorage,                       # large - one entry per meta stream. key and value are fixed length
// IScavengeMap<TStreamId, MetastreamData> metaCollisionStorage,          # very small (probably empty). key is variable length
// IScavengeMap<ulong, OriginalStreamData> originalStorage,               # large - one entry per original-stream. key and value are fixed length
// IScavengeMap<TStreamId, OriginalStreamData> originalCollisionStorage,  # very small (probably empty). key is variable length
// IScavengeMap<int, ChunkTimeStampRange> chunkTimeStampRanges,           # fairly small - one entry per logical chunk. key and value are fixed length
// IScavengeMap<int, float> chunkWeights                                  # fairly small - one entry per logical chunk. key and value are fixed length

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite
{
	public class SqliteScavengeMap<TKey, TValue> : AbstractSqliteBase, IScavengeMap<TKey, TValue> {
		private readonly Dictionary<Type, string> _sqliteTypeMap = new Dictionary<Type, string>() {
			{typeof(int), nameof(SqliteType.Integer)},
			{typeof(float), nameof(SqliteType.Real)},
			{typeof(ulong), nameof(SqliteType.Integer)},
			{typeof(string), nameof(SqliteType.Text)},
		};

		protected string TableName { get; }
		
		public SqliteScavengeMap(string name, SqliteConnection connection) : base(connection) {
			TableName = name;
			AssertTypesAreSupported();
		}

		private void AssertTypesAreSupported() {
			if (!IsSupportedType<TKey>()) {
				throw new ArgumentException($"Unsupported type {nameof(TKey)} for key specified");
			}
			
			if (!IsSupportedType<TValue>()) {
				throw new ArgumentException($"Unsupported type {nameof(TValue)} for value specified");
			}
		}

		protected bool IsSupportedType<T>() {
			return _sqliteTypeMap.ContainsKey(typeof(T));
		}

		public override void Initialize() {
			var keyType = GetSqliteTypeName<TKey>();
			var valueType = GetSqliteTypeName<TValue>();
			var sql = $"CREATE TABLE IF NOT EXISTS {TableName} (key {keyType} PRIMARY KEY, value {valueType} NOT NULL)";
			
			InitializeDb(sql);
		}

		public TValue this[TKey key] {
			set => AddValue(key, value);
		}

		private void AddValue(TKey key, TValue value) {
			var sql = $"INSERT INTO {TableName} VALUES($key, $value)";
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", key);
				parameters.AddWithValue("$value", value);
			});
		}

		public bool TryGetValue(TKey key, out TValue value) {
			var sql = $"SELECT value FROM {TableName} WHERE key = $key";
			return ExecuteSingleRead(sql, parameters => {
				parameters.AddWithValue("$key", key);
			}, reader => reader.GetFieldValue<TValue>(0), out value);
		}

		public bool TryRemove(TKey key, out TValue value) {
			var selectSql = $"SELECT value FROM {TableName} WHERE key = $key";
			var deleteSql = $"DELETE FROM {TableName} WHERE key = $key";
			
			return ExecuteReadAndDelete(selectSql, deleteSql, parameters => {
				parameters.AddWithValue("$key", key);
			}, reader => reader.GetFieldValue<TValue>(0), out value);
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> FromCheckpoint(TKey checkpoint) {
			var sql = $"SELECT key, value FROM {TableName} WHERE key > $key";

			return ExecuteReader(sql, parameters => {
				parameters.AddWithValue("$key", checkpoint);
			}, reader => new KeyValuePair<TKey, TValue>(
				reader.GetFieldValue<TKey>(0), reader.GetFieldValue<TValue>(1)));
		}

		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
			var sql = $"SELECT key, value FROM {TableName}";
			return ExecuteReader(sql, p => { }, reader => new KeyValuePair<TKey, TValue>(
				reader.GetFieldValue<TKey>(0), reader.GetFieldValue<TValue>(1))).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}
