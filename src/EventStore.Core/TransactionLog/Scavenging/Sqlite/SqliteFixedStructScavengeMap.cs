using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite
{
	public class SqliteFixedStructScavengeMap<TKey, TValue> :
		AbstractSqliteBase,
		IScavengeMap<TKey, TValue> where TValue : struct {
		
		private readonly byte[] _buffer;

		private readonly Dictionary<Type, string> _sqliteTypeMap = new Dictionary<Type, string>() {
			{typeof(int), nameof(SqliteType.Integer)},
			{typeof(float), nameof(SqliteType.Real)},
			{typeof(ulong), nameof(SqliteType.Integer)},
			{typeof(string), nameof(SqliteType.Text)},
		};

		public SqliteFixedStructScavengeMap(string name, SqliteConnection connection) : base(connection) {
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

		public override void Initialize() {
			var keyType = GetSqliteTypeName<TKey>();
			var valueType = GetSqliteTypeName<TValue>();
			var sql = $"CREATE TABLE IF NOT EXISTS {TableName} (key {keyType} PRIMARY KEY, value {valueType} NOT NULL)";
			
			InitializeDb(sql);
		}
		
		protected override string GetSqliteTypeName<T>() {
			return typeof(T).IsValueType && !typeof(T).IsPrimitive
				? nameof(SqliteType.Blob)
				: base.GetSqliteTypeName<T>();	
		}

		public TValue this[TKey key] {
			set => AddValue(key, value);
		}

		private void AddValue(TKey key, TValue value) {
			var sql = $"INSERT INTO {TableName} VALUES($key, $value)";
			
			MemoryMarshal.Write(_buffer, ref value);
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", key);
				parameters.AddWithValue("$value", _buffer);
			});
		}

		public bool TryGetValue(TKey key, out TValue value) {
			var sql = $"SELECT value FROM {TableName} WHERE key = $key";
			return ExecuteSingleRead(sql, parameters => {
				parameters.AddWithValue("$key", key);
			}, reader => GetValueField(0, reader), out value);
		}

		public bool TryRemove(TKey key, out TValue value) {
			var selectSql = $"SELECT value FROM {TableName} WHERE key = $key";
			var deleteSql = $"DELETE FROM {TableName} WHERE key = $key";
			
			return ExecuteReadAndDelete(selectSql, deleteSql, parameters => {
				parameters.AddWithValue("$key", key);
			}, reader => GetValueField(0, reader), out value);
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> FromCheckpoint(TKey checkpoint) {
			var sql = $"SELECT key, value FROM {TableName} WHERE key > $key";

			return ExecuteReader(sql, parameters => {
				parameters.AddWithValue("$key", checkpoint);
			}, reader => new KeyValuePair<TKey, TValue>(
				reader.GetFieldValue<TKey>(0), GetValueField(1, reader)));
		}

		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
			var sql = $"SELECT key, value FROM {TableName}";
			return ExecuteReader(sql, p => { }, reader => new KeyValuePair<TKey, TValue>(
				reader.GetFieldValue<TKey>(0), GetValueField(1, reader))).GetEnumerator();
		}
		
		private TValue GetValueField(int ordinal, SqliteDataReader r) {
			var bytes = (byte[])r.GetValue(ordinal);
			return MemoryMarshal.Read<TValue>(bytes);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}
