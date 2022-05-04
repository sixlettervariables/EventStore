using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteChunkWeightScavengeMap :
		SqliteScavengeMap<int, float>,
		IChunkWeightScavengeMap {

		private const string MapName = "ChunkWeightScavengeMap";

		public SqliteChunkWeightScavengeMap(SqliteConnection connection) : base(MapName, connection) {
		}

		public void IncreaseWeight(int logicalChunkNumber, float extraWeight) {
			var sql = $"INSERT INTO {TableName}(key, value) VALUES($key, $value) " +
			          "ON CONFLICT(key) DO UPDATE SET value=value+$value";
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", logicalChunkNumber);
				parameters.AddWithValue("$value", extraWeight);
			});
		}

		public float SumChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber) {
			var sql = $"SELECT SUM(value) FROM {TableName} WHERE key BETWEEN $start AND $end";
			
			ExecuteSingleRead(sql, parameters => {
				parameters.AddWithValue("start", startLogicalChunkNumber);
				parameters.AddWithValue("end", endLogicalChunkNumber);
			}, reader => reader.IsDBNull(0) ? 0 : reader.GetFloat(0), out var value);

			return value;
		}

		public void ResetChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber) {
			var sql = $"DELETE FROM {TableName} WHERE key BETWEEN $start AND $end";
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("start", startLogicalChunkNumber);
				parameters.AddWithValue("end", endLogicalChunkNumber);
			});
		}
	}
}
