namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteChunkWeightScavengeMap :
		SqliteScavengeMap<int, float>,
		IChunkWeightScavengeMap {

		private const string MapName = "ChunkWeightScavengeMap";

		public SqliteChunkWeightScavengeMap(string dir) : base(MapName, dir) {
		}

		public void IncreaseWeight(int logicalChunkNumber, float extraWeight) {
			var sql = $"INSERT INTO {TableName}(key, value) VALUES($key, $value) " +
			          "ON CONFLICT(key) DO UPDATE SET value=value+$value";
			
			ExecuteNonQuery(sql, parameters => {
				parameters.AddWithValue("$key", logicalChunkNumber);
				parameters.AddWithValue("$value", extraWeight);
			});
		}
	}
}
