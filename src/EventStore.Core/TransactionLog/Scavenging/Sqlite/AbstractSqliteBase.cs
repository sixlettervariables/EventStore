using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public abstract class AbstractSqliteBase {
		private readonly SqliteConnection _connection;
		private readonly Dictionary<Type, string> _sqliteTypeMap = new Dictionary<Type, string>() {
			{typeof(int), nameof(SqliteType.Integer)},
			{typeof(float), nameof(SqliteType.Real)},
			{typeof(ulong), nameof(SqliteType.Integer)},
			{typeof(string), nameof(SqliteType.Text)},
		};

		public AbstractSqliteBase(SqliteConnection connection) {
			_connection = connection;
		}

		public abstract void Initialize();
		
		protected void InitializeDb(string createSql) {
			var createTableCmd = _connection.CreateCommand();
			createTableCmd.CommandText = createSql;
			createTableCmd.ExecuteNonQuery();
		}

		protected virtual string GetSqliteTypeName<T>() {
			return _sqliteTypeMap[typeof(T)];
		}

		protected bool ExecuteReadAndDelete<TValue>(string selectSql, string deleteSql,
			Action<SqliteParameterCollection> addParams,
			Func<SqliteDataReader, TValue> getValue, out TValue value) {
			
			if (ExecuteSingleRead(selectSql, addParams, getValue, out value)) {
				var affectedRows = ExecuteNonQuery(deleteSql, addParams);
				
				if (affectedRows == 1) {
					return true;
				} 
				if (affectedRows > 1) {
					throw new SystemException($"More values removed then expected!");
				}
			}

			value = default;
			return false;
		}
		
		protected int ExecuteNonQuery(string sql, Action< SqliteParameterCollection> addParams)
		{
			var cmd = _connection.CreateCommand();
			cmd.CommandText = sql;
			addParams(cmd.Parameters);
			
			try
			{
				return cmd.ExecuteNonQuery();
			}
			catch (SqliteException e) when (e.SqliteErrorCode == 19)
			{
				throw new ArgumentException();
			}
		}

		protected bool ExecuteSingleRead<TValue>(string sql, Action<SqliteParameterCollection> addParams,
			Func<SqliteDataReader, TValue> getValue, out TValue value) {
			
			var selectCmd = _connection.CreateCommand();
			selectCmd.CommandText = sql;
			addParams(selectCmd.Parameters);

			using (var reader = selectCmd.ExecuteReader()) {
				if (reader.Read()) {
					value = getValue(reader);
					return true;
				}
			}
			
			value = default;
			return false;
		}

		protected IEnumerable<KeyValuePair<TKey, TValue>> ExecuteReader<TKey,TValue>(string sql,
			Action<SqliteParameterCollection> addParams,
			Func<SqliteDataReader,KeyValuePair<TKey, TValue>> toValue) {
			
			var cmd = _connection.CreateCommand();
			cmd.CommandText = sql;
			addParams(cmd.Parameters);
			
			using (var reader = cmd.ExecuteReader()) {
				while (reader.Read()) {
					yield return toValue(reader);
				}
			}
		}

		protected static T GetNullableFieldValue<T>(int ordinal, SqliteDataReader reader)
		{
			if (!reader.IsDBNull(ordinal)) {
				return reader.GetFieldValue<T>(ordinal);				
			}

			return default;
		}
		
		protected static void AddNullableParamValue<T>(string param, SqliteParameterCollection parameters, T? value) where T : struct {
			if (value.HasValue) {
				parameters.AddWithValue(param, value.Value);					
			} else {
				parameters.AddWithValue(param, DBNull.Value);					
			}
		}
	}
}
