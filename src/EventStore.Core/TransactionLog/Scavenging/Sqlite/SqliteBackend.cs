using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteBackend {
		private readonly SqliteConnection _connection;
		private SqliteTransaction _transaction;

		public SqliteBackend(SqliteConnection connection) {
			_connection = connection;
		}

		public void InitializeDb(string createSql) {
			var createTableCmd = _connection.CreateCommand();
			createTableCmd.CommandText = createSql;
			createTableCmd.ExecuteNonQuery();
		}

		public SqliteCommand CreateCommand() {
			return _connection.CreateCommand();
		}

		public bool ExecuteReadAndDelete<TValue>(SqliteCommand selectCmd, SqliteCommand deleteCmd,
			Func<SqliteDataReader, TValue> getValue, out TValue value) {
			
			selectCmd.Transaction = _transaction;
			deleteCmd.Transaction = _transaction;
			
			if (ExecuteSingleRead(selectCmd, getValue, out value)) {
				var affectedRows = ExecuteNonQuery(deleteCmd);
				
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

		public int ExecuteNonQuery(SqliteCommand cmd) {
			try {
				cmd.Transaction = _transaction;
				return cmd.ExecuteNonQuery();
			}
			catch (SqliteException e) when (e.SqliteErrorCode == 19) { // Duplicate key error
				throw new ArgumentException();
			}
		}

		public bool ExecuteSingleRead<TValue>(SqliteCommand cmd, Func<SqliteDataReader, TValue> getValue, out TValue value) {
			cmd.Transaction = _transaction;
			using (var reader = cmd.ExecuteReader()) {
				if (reader.Read()) {
					value = getValue(reader);
					return true;
				}
			}
			
			value = default;
			return false;
		}

		public IEnumerable<KeyValuePair<TKey, TValue>> ExecuteReader<TKey,TValue>(SqliteCommand cmd,
			Func<SqliteDataReader,KeyValuePair<TKey, TValue>> toValue) {
			
			cmd.Transaction = _transaction;
			using (var reader = cmd.ExecuteReader()) {
				while (reader.Read()) {
					yield return toValue(reader);
				}
			}
		}

		public static T GetNullableFieldValue<T>(int ordinal, SqliteDataReader reader) {
			if (!reader.IsDBNull(ordinal)) {
				return reader.GetFieldValue<T>(ordinal);				
			}

			return default;
		}

		public SqliteTransaction BeginTransaction() {
			_transaction = _connection.BeginTransaction();
			return _transaction;
		}

		public void ClearTransaction() {
			_transaction = null;
		}
	}
}
