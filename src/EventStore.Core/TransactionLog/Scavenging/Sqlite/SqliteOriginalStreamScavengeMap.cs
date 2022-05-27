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
		private GetChunkExecutionInfoCommand _getChunkExecutionInfo;
		private DeleteCommand _delete;
		private DeleteManyCommand _deleteMany;
		private FromCheckpointCommand _fromCheckpoint;
		private AllRecordsCommand _all;
		private ActiveRecordsCommand _active;

		private string TableName { get; }
		
		public SqliteOriginalStreamScavengeMap(string name) {
			TableName = name;
		}

		public void Initialize(SqliteBackend sqlite) {
			//qq suspect maxAge will be much smaller if we store it as a number. same for the chunk timestamp ranges
			//qq might be sensible to give the discard points defaults of 0
			var sql = $@"
				CREATE TABLE IF NOT EXISTS {TableName} (
					key {SqliteTypeMapping.GetTypeName<TKey>()} PRIMARY KEY,
					isTombstoned      INTEGER DEFAULT 0,
					maxAge            TEXT NULL,
					maxCount          INTEGER NULL,
					truncateBefore    INTEGER NULL,
					discardPoint      INTEGER NULL,
					maybeDiscardPoint INTEGER NULL,
					status            INTEGER DEFAULT 0)";
		
			sqlite.InitializeDb(sql);

			_add = new AddCommand(TableName, sqlite);
			_setTombstone = new SetTombstoneCommand(TableName, sqlite);
			_setMetadata = new SetMetadataCommand(TableName, sqlite);
			_setDiscardPoints = new SetDiscardPointsCommand(TableName, sqlite);
			_get = new GetCommand(TableName, sqlite);
			_getChunkExecutionInfo = new GetChunkExecutionInfoCommand(TableName, sqlite);
			_delete = new DeleteCommand(TableName, sqlite);
			_deleteMany = new DeleteManyCommand(TableName, sqlite);
			_fromCheckpoint = new FromCheckpointCommand(TableName, sqlite);
			_all = new AllRecordsCommand(TableName, sqlite);
			_active = new ActiveRecordsCommand(TableName, sqlite);
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
			_setTombstone.Execute(key);
		}

		public void SetMetadata(TKey key, StreamMetadata metadata) {
			_setMetadata.Execute(key, metadata);
		}
		
		public void SetDiscardPoints(TKey key, CalculationStatus status, DiscardPoint discardPoint, DiscardPoint maybeDiscardPoint) {
			_setDiscardPoints.Execute(key, status, discardPoint, maybeDiscardPoint);
		}

		public bool TryGetChunkExecutionInfo(TKey key, out ChunkExecutionInfo details) {
			return _getChunkExecutionInfo.TryExecute(key, out details);
		}

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> AllRecords() {
			return _all.Execute();
		}

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> ActiveRecords() {
			return _active.Execute();
		}

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> ActiveRecordsFromCheckpoint(TKey checkpoint) {
			return _fromCheckpoint.Execute(checkpoint);
		}

		public void DeleteMany(bool deleteArchived) {
			_deleteMany.Execute(deleteArchived);
		}

		private static OriginalStreamData ReadOriginalStreamData(SqliteDataReader reader) {
			var d = new OriginalStreamData();
			d.IsTombstoned = reader.GetBoolean(0);
			d.MaxAge = SqliteBackend.GetNullableFieldValue<TimeSpan?>(1, reader);
			d.MaxCount = SqliteBackend.GetNullableFieldValue<long?>(2, reader); 
			d.TruncateBefore = SqliteBackend.GetNullableFieldValue<long?>(3, reader);
			d.Status = reader.GetFieldValue<CalculationStatus>(6);

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
			private readonly SqliteParameter _statusParam;

			public AddCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {tableName} VALUES(
						$key,
						$isTombstoned,
						$maxAge,
						$maxCount,
						$truncateBefore,
						$discardPoint,
						$maybeDiscardPoint,
						$status)
					ON CONFLICT(key) DO UPDATE SET
						isTombstoned = $isTombstoned,
						maxAge = $maxAge,
						maxCount = $maxCount,
						truncateBefore = $truncateBefore,
						discardPoint = $discardPoint,
						maybeDiscardPoint = $maybeDiscardPoint,
						status = $status";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_isTombstonedParam = _cmd.Parameters.Add("$isTombstoned", SqliteType.Integer);
				_maxAgeParam = _cmd.Parameters.Add("$maxAge", SqliteType.Text);
				_maxCountParam = _cmd.Parameters.Add("$maxCount", SqliteType.Integer);
				_truncateBeforeParam = _cmd.Parameters.Add("$truncateBefore", SqliteType.Integer);
				_discardPointParam = _cmd.Parameters.Add("$discardPoint", SqliteType.Integer);
				_maybeDiscardPointParam = _cmd.Parameters.Add("$maybeDiscardPoint", SqliteType.Integer);
				_statusParam = _cmd.Parameters.Add("$status", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key, OriginalStreamData value) {
				_keyParam.Value = key;
				_isTombstonedParam.Value = value.IsTombstoned;
				_maxAgeParam.Value = value.MaxAge.HasValue ? (object)value.MaxAge : DBNull.Value;
				_maxCountParam.Value = value.MaxCount.HasValue ? (object)value.MaxCount : DBNull.Value;
				_truncateBeforeParam.Value =
					value.TruncateBefore.HasValue ? (object)value.TruncateBefore : DBNull.Value;
				_discardPointParam.Value = value.DiscardPoint.FirstEventNumberToKeep;
				_maybeDiscardPointParam.Value = value.MaybeDiscardPoint.FirstEventNumberToKeep;
				_statusParam.Value = value.Status;

				_sqlite.ExecuteNonQuery(_cmd);
			}
		}
		
		private class SetTombstoneCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _statusParam;

			public SetTombstoneCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {tableName} (key, isTombstoned, status)
					VALUES ($key, 1, $status)
					ON CONFLICT(key) DO UPDATE SET
						isTombstoned = 1,
						status = $status";
				
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_statusParam = _cmd.Parameters.Add("$status", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key) {
				_keyParam.Value = key;
				_statusParam.Value = CalculationStatus.Active; //qq can be part of the sql instead of param
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
			private readonly SqliteParameter _statusParam;

			public SetMetadataCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {tableName} (key, maxAge, maxCount, truncateBefore, status)
					VALUES($key, $maxAge, $maxCount, $truncateBefore, $status)
					ON CONFLICT(key) DO UPDATE SET
						maxAge = $maxAge,
						maxCount = $maxCount,
						truncateBefore = $truncateBefore,
						status = $status";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_maxAgeParam = _cmd.Parameters.Add("$maxAge", SqliteType.Text);
				_maxCountParam = _cmd.Parameters.Add("$maxCount", SqliteType.Integer);
				_truncateBeforeParam = _cmd.Parameters.Add("$truncateBefore", SqliteType.Integer);
				_statusParam = _cmd.Parameters.Add("$status", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key, StreamMetadata value) {
				_keyParam.Value = key;
				_maxAgeParam.Value = value.MaxAge.HasValue ? (object)value.MaxAge : DBNull.Value;
				_maxCountParam.Value = value.MaxCount.HasValue ? (object)value.MaxCount : DBNull.Value;
				_truncateBeforeParam.Value = value.TruncateBefore.HasValue ? (object)value.TruncateBefore : DBNull.Value;
				_statusParam.Value = CalculationStatus.Active; //qq can be part of the sql instead of param
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}

		private class SetDiscardPointsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _discardPointParam;
			private readonly SqliteParameter _maybeDiscardPointParam;
			private readonly SqliteParameter _statusParam;

			public SetDiscardPointsCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {tableName} (key, discardPoint, maybeDiscardPoint, status)
					VALUES ($key, $discardPoint, $maybeDiscardPoint, $status)
					ON CONFLICT(key) DO UPDATE SET
						discardPoint = $discardPoint,
						maybeDiscardPoint = $maybeDiscardPoint,
						status = $status";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_discardPointParam = _cmd.Parameters.Add("$discardPoint", SqliteType.Integer);
				_maybeDiscardPointParam = _cmd.Parameters.Add("$maybeDiscardPoint", SqliteType.Integer);
				_statusParam = _cmd.Parameters.Add("$status", SqliteType.Integer);
				_cmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute(TKey key, CalculationStatus status, DiscardPoint discardPoint, DiscardPoint maybeDiscardPoint) {
				_keyParam.Value = key;
				_discardPointParam.Value = discardPoint.FirstEventNumberToKeep;
				_maybeDiscardPointParam.Value = maybeDiscardPoint.FirstEventNumberToKeep;
				_statusParam.Value = status;
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}

		private class GetCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public GetCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, status
					FROM {tableName}
					WHERE key = $key";

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

		private class GetChunkExecutionInfoCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public GetChunkExecutionInfoCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					SELECT isTombstoned, maxAge, discardPoint, maybeDiscardPoint
					FROM {tableName}
					WHERE key = $key AND discardPoint IS NOT NULL AND maybeDiscardPoint IS NOT NULL";
				//qq ^ suspicious about the ANDs in the where clause, but these will go away if we
				// give the discard points default values

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public bool TryExecute(TKey key, out ChunkExecutionInfo value) {
				_keyParam.Value = key;
				return _sqlite.ExecuteSingleRead(_cmd, reader => {
					var isTombstoned = reader.GetBoolean(0);
					var maxAge = SqliteBackend.GetNullableFieldValue<TimeSpan?>(1, reader);
					var discardPoint = DiscardPoint.DiscardBefore(reader.GetFieldValue<long>(2));
					var maybeDiscardPoint = DiscardPoint.DiscardBefore(reader.GetFieldValue<long>(3));
					return new ChunkExecutionInfo(isTombstoned, discardPoint, maybeDiscardPoint, maxAge);
				}, out value);
			}
		}

		private class DeleteCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _selectCmd;
			private readonly SqliteCommand _deleteCmd;
			private readonly SqliteParameter _selectKeyParam;
			private readonly SqliteParameter _deleteKeyParam;

			public DeleteCommand(string tableName, SqliteBackend sqlite) {
				var selectSql = $@"
					SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, status
					FROM {tableName}
					WHERE key = $key";

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
		
		private class DeleteManyCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _deleteCmd;
			private readonly SqliteParameter _spentStatusParam;
			private readonly SqliteParameter _archiveStatusParam;

			public DeleteManyCommand(string tableName, SqliteBackend sqlite) {
				var deleteSql = $@"
					DELETE FROM {tableName}
					WHERE status = $spent OR status = $archive";

				_deleteCmd = sqlite.CreateCommand();
				_deleteCmd.CommandText = deleteSql;
				_spentStatusParam = _deleteCmd.Parameters.Add("$spent", SqliteType.Integer);
				_archiveStatusParam = _deleteCmd.Parameters.Add("$archive", SqliteType.Integer);
				_deleteCmd.Prepare();
				
				_sqlite = sqlite;
			}

			public void Execute(bool deleteArchived) {
				_spentStatusParam.Value = CalculationStatus.Spent;
				//qq ^ can be directly in the sql {(int)CalculationStatus.Spent}
				// only delete archived when requested, otherwise only delete Spent.
				_archiveStatusParam.Value = deleteArchived ? CalculationStatus.Archived : CalculationStatus.Spent;
				_sqlite.ExecuteNonQuery(_deleteCmd);
			}
		}

		// the sqlite docs (https://www.sqlite.org/lang_select.html) say that without an orderby clause
		// the order in which the rows are returned is undefined. so we include an orderby clause
		// in FromCheckpointCommand and ActiveRecordsCommand to facilitate checkpointing and continuing
		//
		// the sqlite docs (https://www.sqlite.org/isolation.html) say that on a single connection the
		// behaviour of running insert/update/delete during a select is safe, but undefined with respect
		// to whether the updates will appear in the select. in particular, updating the current row
		// may cause the row to reappear later in the select. we are ordering the select by key so in
		// our case it seems doubtful that it will reappear because it would violate the orderby clause,
		// but in the worse case we will just duplicate the effort but otherwise no harm is done.
		//
		// todo: we could measure to see whether an additional index on (status, key) would help.
		//  - it would slow down inserts/updates to maintain the index
		//  - it would speed up iteration, especially if there are lots of archived records
		//  - the orderby might need to change to (status, key) in order to use the index
		//    which might have implications for the sqlite docs comments above
		//  - 

		//qq in general we want to lean towards reads being faster because we will read more than we will write.
		// because we will keep re-reading the data in subsequent scavenges.
		//  but consider/measure whether it is worth clustering on rowid instead of the key, and having a separate
		//  index as the key. i _think_ this can be achieved by simply using INT instead of INTEGER as the key type.
		//  it would make inserts faster but reads slower. and the database bigger.
		private class FromCheckpointCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly SqliteParameter _statusParam;

			public FromCheckpointCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, status, key
					FROM {tableName}
					WHERE key > $key AND status = $status
					ORDER BY key";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<TKey>());
				_statusParam = _cmd.Parameters.Add("$status", SqliteType.Integer);
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> Execute(TKey key) {
				_keyParam.Value = key;
				_statusParam.Value = CalculationStatus.Active;
				//qq ^ can be direct in sql
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, OriginalStreamData>(
					reader.GetFieldValue<TKey>(7), ReadOriginalStreamData(reader)));
			}
		}

		private class AllRecordsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;

			public AllRecordsCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, status, key
					FROM {tableName}";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> Execute() {
				return _sqlite.ExecuteReader(_cmd, reader => new KeyValuePair<TKey, OriginalStreamData>(
					reader.GetFieldValue<TKey>(7), ReadOriginalStreamData(reader)));
			}
		}
		
		private class ActiveRecordsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _statusParam;

			public ActiveRecordsCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					SELECT isTombstoned, maxAge, maxCount, truncateBefore, discardPoint, maybeDiscardPoint, status, key
					FROM {tableName}
					WHERE status = $status
					ORDER BY key";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_statusParam = _cmd.Parameters.Add("$status", SqliteType.Integer);
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> Execute() {
				_statusParam.Value = CalculationStatus.Active;
				//qq ^ can be direct in sql
				return _sqlite.ExecuteReader(
					_cmd,
					//qq i wonder if reading the fields in order makes a difference to performance
					// we could always read the value first into a local variable and then the key
					reader => new KeyValuePair<TKey, OriginalStreamData>(
						reader.GetFieldValue<TKey>(7),
						ReadOriginalStreamData(reader)));
			}
		}
	}
}
