using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteFixedStructScavengeMap<TKey, TValue> :
		ISqliteScavengeBackend,
		IScavengeMap<TKey, TValue> where TValue : struct {
		
		private AddCommand _add;
		private GetCommand _get;
		private RemoveCommand _delete;
		private AllRecordsCommand _all;
		private readonly byte[] _buffer;

		private readonly Dictionary<Type, string> _sqliteTypeMap = new Dictionary<Type, string>() {
			{typeof(int), nameof(SqliteType.Integer)},
			{typeof(float), nameof(SqliteType.Real)},
			{typeof(ulong), nameof(SqliteType.Integer)},
			{typeof(string), nameof(SqliteType.Text)},
		};

		public SqliteFixedStructScavengeMap(string name) {
			if (typeof(TValue).IsPrimitive) {
				throw new ArgumentException($"Invalid type for value, use {nameof(SqliteScavengeMap<TKey,TValue>)} for primitive types!");
			}

			_buffer = new byte[Marshal.SizeOf(typeof(TValue))];
			TableName = name;
			
			AssertTypesAreSupported();
		}
		
		private string TableName { get; }

		private void AssertTypesAreSupported() {
			if (!IsSupportedType<TKey>()) {
				throw new ArgumentException($"Unsupported type {nameof(TKey)} for key specified");
			}
			
			if (!IsSupportedType<TValue>()) {
				throw new ArgumentException($"Unsupported type {nameof(TValue)} for value specified");
			}
		}

		protected bool IsSupportedType<T>() {
			if (typeof(T).IsValueType && !typeof(T).IsPrimitive) {
				return true;
			}
			
			return _sqliteTypeMap.ContainsKey(typeof(T));
		}

		public void Initialize(SqliteBackend sqlite) {
			var keyType = SqliteTypeMapping.GetTypeName<TKey>();
			var sql = $"CREATE TABLE IF NOT EXISTS {TableName} (key {keyType} PRIMARY KEY, value Blob NOT NULL)";
			sqlite.InitializeDb(sql);
			
			_add = new AddCommand(TableName, sqlite);
			_get = new GetCommand(TableName, sqlite);
			_delete = new RemoveCommand(TableName, sqlite);
			_all = new AllRecordsCommand(TableName, sqlite);
		}

		public TValue this[TKey key] {
			set => AddValue(key, value);
		}

		private void AddValue(TKey key, TValue value) {
			//qq if this is endianness specific (it is i think) lets add a startup check along the lines of
			//if (!BitConverter.IsLittleEndian) {
			//	throw new NotSupportedException();
			//}
			MemoryMarshal.Write(_buffer, ref value);
			_add.Execute(key, _buffer);
		}

		public bool TryGetValue(TKey key, out TValue value) {
			return _get.TryExecute(key, out value);
		}

		public bool TryRemove(TKey key, out TValue value) {
			return _delete.TryExecute(key, out value);
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> AllRecords() {
			return _all.Execute();
		}

		private static TValue GetValueField(int ordinal, SqliteDataReader r) {
			var bytes = (byte[])r.GetValue(ordinal);
			return MemoryMarshal.Read<TValue>(bytes);
		}
		
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
				_valueParam = _cmd.Parameters.Add("$value", SqliteType.Blob);
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public void Execute(TKey key, byte[] value) {
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
				//qq closure allocation (other places too)
				return _sqlite.ExecuteSingleRead(_cmd, reader => GetValueField(0, reader), out value);
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
					reader => GetValueField(0, reader), out value);
			}
		}

		private class AllRecordsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;

			public AllRecordsCommand(string tableName, SqliteBackend sqlite) {
				var sql = $"SELECT key, value FROM {tableName}";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, TValue>> Execute() {
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, TValue>(
					reader.GetFieldValue<TKey>(0), GetValueField(1, reader)));
			}
		}
	}
}
