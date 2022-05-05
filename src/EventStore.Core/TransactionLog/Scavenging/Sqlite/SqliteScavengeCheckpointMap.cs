using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteScavengeCheckpointMap<TStreamId>: AbstractSqliteBase, IScavengeMap<Unit, ScavengeCheckpoint> {
		public SqliteScavengeCheckpointMap(SqliteConnection connection) : base(connection) {
		}
		
		public override void Initialize() {
			var sql = "CREATE TABLE IF NOT EXISTS ScavengeCheckpointMap (key Integer PRIMARY KEY, value Text NOT NULL)";
			InitializeDb(sql);
		}

		public ScavengeCheckpoint this[Unit key] {
			set => AddValue(key, value);
		}

		protected void AddValue(Unit _, ScavengeCheckpoint value) {
			var sql = "INSERT INTO ScavengeCheckpointMap VALUES(0, $value) ON CONFLICT(key) DO UPDATE SET value=$value";
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$value", ScavengeCheckpointJsonPersistence<TStreamId>.Serialize(value));
			});
		}

		public bool TryGetValue(Unit key, out ScavengeCheckpoint value) {
			var sql = "SELECT value FROM ScavengeCheckpointMap WHERE key = 0";
			return ExecuteSingleRead(sql, p => { }, reader => {
				//qq handle false
				ScavengeCheckpointJsonPersistence<TStreamId>.TryDeserialize(reader.GetString(0), out var v);
				return v;
			}, out value);
		}

		public bool TryRemove(Unit key, out ScavengeCheckpoint value) {
			var selectSql = "SELECT value FROM ScavengeCheckpointMap WHERE key = 0";
			var deleteSql = "DELETE FROM ScavengeCheckpointMap WHERE key = 0";
			return ExecuteReadAndDelete(selectSql, deleteSql, parameters => { },
				reader => {
					//qq handle false
					ScavengeCheckpointJsonPersistence<TStreamId>.TryDeserialize(reader.GetString(0), out var v);
					return v;
				}, out value);
		}

		public IEnumerable<KeyValuePair<Unit, ScavengeCheckpoint>> FromCheckpoint(Unit checkpoint) {
			throw new NotImplementedException();
		}

		public IEnumerator<KeyValuePair<Unit, ScavengeCheckpoint>> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}
