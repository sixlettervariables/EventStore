using System;
using System.Collections;
using System.Collections.Generic;
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
	public class SqliteScavengeMap<TKey, TValue> : ISqliteScavengeBackend, IScavengeMap<TKey, TValue> {
		private AddCommand _add;
		private GetCommand _get;
		private RemoveCommand _delete;
		private FromCheckpointCommand _fromCheckpoint;
		private EnumeratorCommand _enumerator;

		private readonly Dictionary<Type, string> _sqliteTypeMap = new Dictionary<Type, string>() {
			{typeof(int), nameof(SqliteType.Integer)},
			{typeof(float), nameof(SqliteType.Real)},
			{typeof(ulong), nameof(SqliteType.Integer)},
			{typeof(string), nameof(SqliteType.Text)},
		};

		protected string TableName { get; }
		
		public SqliteScavengeMap(string name) {
			TableName = name;
			AssertTypesAreSupported();
		}

		private void AssertTypesAreSupported() {
			if (!IsSupportedType<TKey>()) {
				throw new ArgumentException($"Scavenge map {TableName} has an unsupported type {typeof(TKey).Name} for key specified");
			}
			
			if (!IsSupportedType<TValue>()) {
				throw new ArgumentException($"Scavenge map {TableName} has an unsupported type {typeof(TValue).Name} for value specified");
			}
		}

		private bool IsSupportedType<T>() {
			return _sqliteTypeMap.ContainsKey(typeof(T));
		}

		public virtual void Initialize(SqliteBackend sqlite) {
			var keyType = SqliteTypeMapping.GetTypeName<TKey>();
			var valueType = SqliteTypeMapping.GetTypeName<TValue>();
			var createSql = $"CREATE TABLE IF NOT EXISTS {TableName} (key {keyType} PRIMARY KEY, value {valueType} NOT NULL)";
			
			sqlite.InitializeDb(createSql);

			_add = new AddCommand(TableName, sqlite);
			_get = new GetCommand(TableName, sqlite);
			_delete = new RemoveCommand(TableName, sqlite);
			_fromCheckpoint = new FromCheckpointCommand(TableName, sqlite);
			_enumerator = new EnumeratorCommand(TableName, sqlite);
		}
		
		public TValue this[TKey key] {
			set => AddValue(key, value);
		}

		private void AddValue(TKey key, TValue value) {
			_add.Execute(key, value);
		}

		public bool TryGetValue(TKey key, out TValue value) {
			return _get.TryExecute(key, out value);
		}

		public bool TryRemove(TKey key, out TValue value) {
			return _delete.TryExecute(key, out value);
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> AllRecords() {
			throw new NotImplementedException(); //qqqqq all records
			//return _enumerator.Execute();
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> ActiveRecords() {
			throw new NotImplementedException(); //qqqqq records whose status is active
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> ActiveRecordsFromCheckpoint(TKey checkpoint) {
			return _fromCheckpoint.Execute(checkpoint);
		}

		//qq remove public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
		//	var sql = $"SELECT key, value FROM {TableName}";
		//	return ExecuteReader(sql, p => { }, reader => new KeyValuePair<TKey, TValue>(
		//		reader.GetFieldValue<TKey>(0), reader.GetFieldValue<TValue>(1))).GetEnumerator();
		//}

		////qq remove IEnumerator IEnumerable.GetEnumerator() {
		//	return GetEnumerator();
		//}
		
		private class AddCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _valueParam;

			public AddCommand(string tableName, SqliteBackend sqlite) {
				var sql = $"INSERT INTO {tableName} VALUES($key, $value) ON CONFLICT(key) DO UPDATE SET value=$value";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_valueParam = _cmd.Parameters.Add("$value", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public void Execute(TKey key, TValue value) {
				_keyParam.Value = key;
				_valueParam.Value = value;
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}
		private class GetCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public GetCommand(string tableName, SqliteBackend sqlite) {
				var selectSql = $"SELECT value FROM {tableName} WHERE key = $key";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = selectSql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public bool TryExecute(TKey key, out TValue value) {
				_keyParam.Value = key;
				return _sqlite.ExecuteSingleRead(_cmd, reader => reader.GetFieldValue<TValue>(0), out value);
			}
		}
		private class RemoveCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _selectCmd;
			private readonly SqliteCommand _deleteCmd;
			private readonly SqliteParameter _selectKeyParam;
			private readonly SqliteParameter _deleteKeyParam;

			public RemoveCommand(string tableName, SqliteBackend sqlite) {
				var selectSql = $"SELECT value FROM {tableName} WHERE key = $key";
				_selectCmd = sqlite.CreateCommand();
				_selectCmd.CommandText = selectSql;
				_selectKeyParam = _selectCmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_selectCmd.Prepare();

				var deleteSql = $"DELETE FROM {tableName} WHERE key = $key";
				_deleteCmd = sqlite.CreateCommand();
				_deleteCmd.CommandText = deleteSql;
				_deleteKeyParam = _deleteCmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_deleteCmd.Prepare();
				
				_sqlite = sqlite;
			}

			public bool TryExecute(TKey key, out TValue value) {
				_selectKeyParam.Value = key;
				_deleteKeyParam.Value = key;
				return _sqlite.ExecuteReadAndDelete(_selectCmd, _deleteCmd,
					reader => reader.GetFieldValue<TValue>(0), out value);
			}
		}
		private class FromCheckpointCommand {
			private readonly SqliteBackend  _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public FromCheckpointCommand(string tableName, SqliteBackend sqlite) {
				var sql = $"SELECT key, value FROM {tableName} WHERE key > $key";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, TValue>> Execute(TKey key) {
				_keyParam.Value = key;
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, TValue>(
					reader.GetFieldValue<TKey>(0), reader.GetFieldValue<TValue>(1)));
			}
		}
		private class EnumeratorCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;

			public EnumeratorCommand(string tableName, SqliteBackend sqlite) {
				var sql = $"SELECT key, value FROM {tableName}";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerator<KeyValuePair<TKey, TValue>> Execute() {
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, TValue>(
					reader.GetFieldValue<TKey>(0), reader.GetFieldValue<TValue>(1))).GetEnumerator();
			}
		}
	}
}
