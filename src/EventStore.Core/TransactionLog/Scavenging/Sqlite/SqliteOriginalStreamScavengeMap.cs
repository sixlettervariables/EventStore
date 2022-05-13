using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteOriginalStreamScavengeMap<TKey> : ISqliteScavengeBackend, IOriginalStreamScavengeMap<TKey> {
		private AddCommand _add;
		private SetTombstoneCommand _setTombstone;
		private SetMetadataCommand _setMetadata;
		private SetDiscardPointsCommand _setDiscardPoints;
		private GetCommand _get;
		private GetStreamExecutionDetailsCommand _getStreamExecutionDetails;
		private RemoveCommand _delete;
		private FromCheckpointCommand _fromCheckpoint;
		private EnumeratorCommand _enumerator;

		private string TableName { get; }
		
		public SqliteOriginalStreamScavengeMap(string name) {
			TableName = name;
		}

		public void Initialize(SqliteBackend sqlite) {
			var sql = 
				$"CREATE TABLE IF NOT EXISTS {TableName} (" +
				$"key {SqliteTypeMapping.GetTypeName<TKey>()} PRIMARY KEY, " +
				"isTombstoned      INTEGER DEFAULT 0, " +
				"maxAge            TEXT NULL, " +
				"maxCount          INTEGER NULL, " +
				"truncateBefore    INTEGER NULL, " +
				"discardPoint      INTEGER NULL, " +
				"maybeDiscardPoint INTEGER NULL)";
		
			sqlite.InitializeDb(sql);

			_add = new AddCommand(TableName, sqlite);
			_setTombstone = new SetTombstoneCommand(TableName, sqlite);
			_setMetadata = new SetMetadataCommand(TableName, sqlite);
			_setDiscardPoints = new SetDiscardPointsCommand(TableName, sqlite);
			_get = new GetCommand(TableName, sqlite);
			_getStreamExecutionDetails = new GetStreamExecutionDetailsCommand(TableName, sqlite);
			_delete = new RemoveCommand(TableName, sqlite);
			_fromCheckpoint = new FromCheckpointCommand(TableName, sqlite);
			_enumerator = new EnumeratorCommand(TableName, sqlite);
		}

		public OriginalStreamData this[TKey key] {
			set => _add.Execute(key, value);
		}

		public bool TryGetValue(TKey key, out OriginalStreamData value) {
			return _get.TryExecute(key, out value);
		}

		public bool TryRemove(TKey key, out OriginalStreamData value) {
			return _delete.TryExecute(key, out value);
		}
		
		public void SetTombstone(TKey key) {
			//qqqq needs to update the status to Active
			_setTombstone.Execute(key);
		}

		public void SetMetadata(TKey key, StreamMetadata metadata) {
			//qqqq needs to update the status to Active
			_setMetadata.Execute(key, metadata);
		}
		
		public void SetDiscardPoints(TKey key, CalculationStatus status, DiscardPoint discardPoint, DiscardPoint maybeDiscardPoint) {
			//qqqq needs to update the status to the param
			_setDiscardPoints.Execute(key, discardPoint, maybeDiscardPoint);
		}
		
		public void SetDiscardPoints(TKey key, DiscardPoint discardPoint, DiscardPoint maybeDiscardPoint) {
			
		}

		public bool TryGetChunkExecutionInfo(TKey key, out ChunkExecutionInfo details) {
			return _getStreamExecutionDetails.TryExecute(key, out details);
		}

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> AllRecords() {
			return _enumerator.Execute();
		}

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> ActiveRecords() {
			throw new NotImplementedException(); //qqqqq records whose status is active
		}

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> ActiveRecordsFromCheckpoint(TKey checkpoint) {
			//qqqq needs to return only active records
			return _fromCheckpoint.Execute(checkpoint);
		}

		public void DeleteMany(bool deleteArchived) {
			throw new NotImplementedException();
			//qqqq delete according to the status and the flags
		}

		private static OriginalStreamData ReadOriginalStreamData(SqliteDataReader reader) {
			var d = new OriginalStreamData();
			d.IsTombstoned = reader.GetBoolean(0);
			d.MaxAge = SqliteBackend.GetNullableFieldValue<TimeSpan?>(1, reader);
			d.MaxCount = SqliteBackend.GetNullableFieldValue<long?>(2, reader); 
			d.TruncateBefore = SqliteBackend.GetNullableFieldValue<long?>(3, reader);

			var discardPoint = SqliteBackend.GetNullableFieldValue<long?>(4, reader);
			if (discardPoint.HasValue) {
				d.DiscardPoint = DiscardPoint.DiscardBefore(discardPoint.Value);					
			}

			var maybeDiscardPoint = SqliteBackend.GetNullableFieldValue<long?>(5, reader);
			if (maybeDiscardPoint.HasValue) {
				d.MaybeDiscardPoint = DiscardPoint.DiscardBefore(maybeDiscardPoint.Value);					
			}

			return d;
		}

		private class AddCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _isTombstonedParam;
			private readonly SqliteParameter _maxAgeParam;
			private readonly SqliteParameter _maxCountParam;
			private readonly SqliteParameter _truncateBeforeParam;
			private readonly SqliteParameter _discardPointParam;
			private readonly SqliteParameter _maybeDiscardPointParam;

			public AddCommand(string tableName, SqliteBackend sqlite) {
				var sql =
					$"INSERT INTO {tableName} VALUES($key, $isTombstoned, $maxAge, $maxCount, $truncateBefore, $discardPoint, $maybeDiscardPoint)" +
					"ON CONFLICT(key) DO UPDATE SET isTombstoned=$isTombstoned, maxAge=$maxAge, maxCount=$maxCount, truncateBefore=$truncateBefore, discardPoint=$discardPoint, maybeDiscardPoint=$maybeDiscardPoint";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_isTombstonedParam = _cmd.Parameters.Add("$isTombstoned", SqliteType.Integer);
				_maxAgeParam = _cmd.Parameters.Add("$maxAge", SqliteType.Text);
				_maxCountParam = _cmd.Parameters.Add("$maxCount", SqliteType.Integer);
				_truncateBeforeParam = _cmd.Parameters.Add("$truncateBefore", SqliteType.Integer);
				_discardPointParam = _cmd.Parameters.Add("$discardPoint", SqliteType.Integer);
				_maybeDiscardPointParam = _cmd.Parameters.Add("$maybeDiscardPoint", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key, OriginalStreamData value) {
				_keyParam.Value = key;
				_isTombstonedParam.Value = value.IsTombstoned;
				//qqq check if ?? is needed
				_maxAgeParam.Value = value.MaxAge.HasValue ? (object)value.MaxAge : DBNull.Value;
				_maxCountParam.Value = value.MaxCount.HasValue ? (object)value.MaxCount : DBNull.Value;
				_truncateBeforeParam.Value =
					value.TruncateBefore.HasValue ? (object)value.TruncateBefore : DBNull.Value;
				_discardPointParam.Value = value.DiscardPoint.FirstEventNumberToKeep;
				_maybeDiscardPointParam.Value = value.MaybeDiscardPoint.FirstEventNumberToKeep;

				_sqlite.ExecuteNonQuery(_cmd);
			}
		}
		
		private class SetTombstoneCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public SetTombstoneCommand(string tableName, SqliteBackend sqlite) {
				var sql =
					$"INSERT INTO {tableName} (key, isTombstoned) VALUES($key, 1) " + 
					"ON CONFLICT(key) DO UPDATE SET isTombstoned=1";
				
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key) {
				_keyParam.Value = key;
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}
		
		private class SetMetadataCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _maxAgeParam;
			private readonly SqliteParameter _maxCountParam;
			private readonly SqliteParameter _truncateBeforeParam;

			public SetMetadataCommand(string tableName, SqliteBackend sqlite) {
				var sql =
					$"INSERT INTO {tableName} (key, maxAge, maxCount, truncateBefore) VALUES($key, $maxAge, $maxCount, $truncateBefore) " + 
					"ON CONFLICT(key) DO UPDATE SET maxAge=$maxAge, maxCount=$maxCount, truncateBefore=$truncateBefore";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_maxAgeParam = _cmd.Parameters.Add("$maxAge", SqliteType.Text);
				_maxCountParam = _cmd.Parameters.Add("$maxCount", SqliteType.Integer);
				_truncateBeforeParam = _cmd.Parameters.Add("$truncateBefore", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key, StreamMetadata value) {
				_keyParam.Value = key;
				//qqq check if ?? is needed
				_maxAgeParam.Value = value.MaxAge.HasValue ? (object)value.MaxAge : DBNull.Value;
				_maxCountParam.Value = value.MaxCount.HasValue ? (object)value.MaxCount : DBNull.Value;
				_truncateBeforeParam.Value = value.TruncateBefore.HasValue ? (object)value.TruncateBefore : DBNull.Value;

				_sqlite.ExecuteNonQuery(_cmd);
			}
		}

		private class SetDiscardPointsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _discardPointParam;
			private readonly SqliteParameter _maybeDiscardPointParam;

			public SetDiscardPointsCommand(string tableName, SqliteBackend sqlite) {
				var sql =
					$"INSERT INTO {tableName} (key, discardPoint, maybeDiscardPoint) VALUES($key, $discardPoint, $maybeDiscardPoint) " + 
					"ON CONFLICT(key) DO UPDATE SET discardPoint=$discardPoint, maybeDiscardPoint=$maybeDiscardPoint";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_discardPointParam = _cmd.Parameters.Add("$discardPoint", SqliteType.Integer);
				_maybeDiscardPointParam = _cmd.Parameters.Add("$maybeDiscardPoint", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key, DiscardPoint discardPoint, DiscardPoint maybeDiscardPoint) {
				_keyParam.Value = key;
				_discardPointParam.Value = discardPoint.FirstEventNumberToKeep;
				_maybeDiscardPointParam.Value = maybeDiscardPoint.FirstEventNumberToKeep;
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}
		
		private class GetCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public GetCommand(string tableName, SqliteBackend sqlite) {
				var sql = "SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint " +
				          $"FROM {tableName} WHERE key = $key";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public bool TryExecute(TKey key, out OriginalStreamData value) {
				_keyParam.Value = key;
				return _sqlite.ExecuteSingleRead(_cmd, ReadOriginalStreamData, out value);
			}
		}

		private class GetStreamExecutionDetailsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public GetStreamExecutionDetailsCommand(string tableName, SqliteBackend sqlite) {
				var sql = $"SELECT maxAge, discardPoint, maybeDiscardPoint FROM {tableName} " +
				          "WHERE key = $key AND discardPoint IS NOT NULL AND maybeDiscardPoint IS NOT NULL";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public bool TryExecute(TKey key, out ChunkExecutionInfo value) {
				_keyParam.Value = key;
				return _sqlite.ExecuteSingleRead(_cmd, reader => {
					var maxAge = SqliteBackend.GetNullableFieldValue<TimeSpan?>(0, reader);
					var discardPoint = DiscardPoint.DiscardBefore(reader.GetFieldValue<long>(1));
					var maybeDiscardPoint = DiscardPoint.DiscardBefore(reader.GetFieldValue<long>(2));
					return new ChunkExecutionInfo(isTombstoned: false, discardPoint, maybeDiscardPoint, maxAge); //qq store istombstoned in the table
				}, out value);
			}
		}

		private class RemoveCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _selectCmd;
			private readonly SqliteCommand _deleteCmd;
			private readonly SqliteParameter _selectKeyParam;
			private readonly SqliteParameter _deleteKeyParam;

			public RemoveCommand(string tableName, SqliteBackend sqlite) {
				var selectSql =
					"SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint " + 
					$"FROM {tableName} WHERE key = $key";
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

			public bool TryExecute(TKey key, out OriginalStreamData value) {
				_selectKeyParam.Value = key;
				_deleteKeyParam.Value = key;
				return _sqlite.ExecuteReadAndDelete(_selectCmd, _deleteCmd, ReadOriginalStreamData, out value);
			}
		}

		private class FromCheckpointCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public FromCheckpointCommand(string tableName, SqliteBackend sqlite) {
				var sql = "SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, key " +
				          $"FROM {tableName} WHERE key > $key";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> Execute(TKey key) {
				_keyParam.Value = key;
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, OriginalStreamData>(
					reader.GetFieldValue<TKey>(6), ReadOriginalStreamData(reader)));
			}
		}
		private class EnumeratorCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;

			public EnumeratorCommand(string tableName, SqliteBackend sqlite) {
				var sql = "SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, key " + 
				          $"FROM {tableName}";
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> Execute() {
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, OriginalStreamData>(
					reader.GetFieldValue<TKey>(6), ReadOriginalStreamData(reader)));
			}
		}
	}
}
