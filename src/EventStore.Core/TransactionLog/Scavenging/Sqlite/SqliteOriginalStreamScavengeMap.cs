using System;
using System.Collections;
using System.Collections.Generic;
using EventStore.Core.Data;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteOriginalStreamScavengeMap<TKey> : AbstractSqliteBase, IOriginalStreamScavengeMap<TKey> {
		private readonly string _insertSql;

		private string TableName { get; }
		
		public SqliteOriginalStreamScavengeMap(string name, SqliteConnection connection) : base(connection) {
			TableName = name;
			_insertSql = $"INSERT INTO {TableName} VALUES($key, $isTombstoned, $maxAge, $maxCount, $truncateBefore, $discardPoint, $maybeDiscardPoint)";
		}

		public override void Initialize() {
			var sql = 
				$"CREATE TABLE IF NOT EXISTS {TableName} (" +
				$"key {GetSqliteTypeName<TKey>()} PRIMARY KEY, " +
				"isTombstoned      INTEGER DEFAULT 0, " +
				"maxAge            TEXT NULL, " +
				"maxCount          INTEGER NULL, " +
				"truncateBefore    INTEGER NULL, " +
				"discardPoint      INTEGER NULL, " +
				"maybeDiscardPoint INTEGER NULL)";
		
			InitializeDb(sql);
		}

		public OriginalStreamData this[TKey key] {
			set => ExecuteNonQuery(_insertSql, parameters => {
				parameters.AddWithValue("$key", key);
				parameters.AddWithValue("$isTombstoned", value.IsTombstoned);
				AddNullableParamValue("$maxAge", parameters, value.MaxAge);
				AddNullableParamValue("$maxCount", parameters, value.MaxCount);
				AddNullableParamValue("$truncateBefore", parameters, value.TruncateBefore);
				parameters.AddWithValue("$discardPoint", value.DiscardPoint.FirstEventNumberToKeep);
				parameters.AddWithValue("$maybeDiscardPoint", value.MaybeDiscardPoint.FirstEventNumberToKeep);
			});
		}

		public bool TryRemove(TKey key, out OriginalStreamData value) {
			var selectSql =
				"SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint " + 
				$"FROM {TableName} WHERE key = $key";
			var deleteSql = "DELETE FROM OriginalStreamScavengeMap WHERE key = $key";
			
			return ExecuteReadAndDelete(selectSql, deleteSql, parameters => {
				parameters.AddWithValue("$key", key);
			}, ReadOriginalStreamData, out value);
		}

		public bool TryGetValue(TKey key, out OriginalStreamData value) {
			var sql = "SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint " +
			          $"FROM {TableName} WHERE key = $key";

			return ExecuteSingleRead(sql, parameters => {
				parameters.AddWithValue("$key", key);
			}, ReadOriginalStreamData, out value);
		}

		public void SetTombstone(TKey key) {
			var sql =
				$"INSERT INTO {TableName} (key, isTombstoned) VALUES($key, 1) " + 
				"ON CONFLICT(key) DO UPDATE SET isTombstoned=1";
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", key);
			});
		}

		public void SetMetadata(TKey key, StreamMetadata metadata) {
			var sql =
				$"INSERT INTO {TableName} (key, maxAge, maxCount, truncateBefore) VALUES($key, $maxAge, $maxCount, $truncateBefore) " + 
				"ON CONFLICT(key) DO UPDATE SET maxAge=$maxAge, maxCount=$maxCount, truncateBefore=$truncateBefore";
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", key);
				AddNullableParamValue("$maxAge", parameters, metadata.MaxAge);
				AddNullableParamValue("$maxCount", parameters, metadata.MaxCount);
				AddNullableParamValue("$truncateBefore", parameters, metadata.TruncateBefore);
			});
		}
		
		public void SetDiscardPoints(TKey key, DiscardPoint discardPoint, DiscardPoint maybeDiscardPoint) {
			var sql =
				$"INSERT INTO {TableName} (key, discardPoint, maybeDiscardPoint) VALUES($key, $discardPoint, $maybeDiscardPoint) " + 
				"ON CONFLICT(key) DO UPDATE SET discardPoint=$discardPoint, maybeDiscardPoint=$maybeDiscardPoint";
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", key);
				parameters.AddWithValue("discardPoint", discardPoint.FirstEventNumberToKeep);
				parameters.AddWithValue("maybeDiscardPoint", maybeDiscardPoint.FirstEventNumberToKeep);
			});
		}

		public bool TryGetStreamExecutionDetails(TKey key, out StreamExecutionDetails details) {
			var sql = $"SELECT maxAge, discardPoint, maybeDiscardPoint FROM {TableName} WHERE key = $key";

			return ExecuteSingleRead(sql, parameters => {
				parameters.AddWithValue("$key", key);
			}, reader => {
				var maxAge = GetNullableFieldValue<TimeSpan?>(0, reader);
				var discardPoint = DiscardPoint.DiscardBefore(reader.GetFieldValue<long>(1));
				var maybeDiscardPoint = DiscardPoint.DiscardBefore(reader.GetFieldValue<long>(2));
				return new StreamExecutionDetails(discardPoint, maybeDiscardPoint, maxAge);
			}, out details);
		}
		
		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> FromCheckpoint(TKey checkpoint) {
			var sql = "SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, key " +
			          $"FROM {TableName} WHERE key > $key";

			return ExecuteReader(sql, parameters => {
				parameters.AddWithValue("$key", checkpoint);
			}, reader => new KeyValuePair<TKey, OriginalStreamData>(
				reader.GetFieldValue<TKey>(6), ReadOriginalStreamData(reader)));
		}

		public IEnumerator<KeyValuePair<TKey, OriginalStreamData>> GetEnumerator() {
			var sql = "SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, key " + "" +
			          $"FROM {TableName}";

			return ExecuteReader(sql, p => {}, reader => new KeyValuePair<TKey, OriginalStreamData>(
				reader.GetFieldValue<TKey>(6), ReadOriginalStreamData(reader))).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
		
		private OriginalStreamData ReadOriginalStreamData(SqliteDataReader reader) {
			var d = new OriginalStreamData();
			d.IsTombstoned = reader.GetBoolean(0);
			d.MaxAge = GetNullableFieldValue<TimeSpan?>(1, reader);
			d.MaxCount = GetNullableFieldValue<long?>(2, reader); 
			d.TruncateBefore = GetNullableFieldValue<long?>(3, reader);

			var discardPoint = GetNullableFieldValue<long?>(4, reader);
			if (discardPoint.HasValue) {
				d.DiscardPoint = DiscardPoint.DiscardBefore(discardPoint.Value);					
			}

			var maybeDiscardPoint = GetNullableFieldValue<long?>(5, reader);
			if (maybeDiscardPoint.HasValue) {
				d.MaybeDiscardPoint = DiscardPoint.DiscardBefore(maybeDiscardPoint.Value);					
			}

			return d;
		}
	}
}
