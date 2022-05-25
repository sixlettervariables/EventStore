using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteDbFixture<T> : IAsyncLifetime {
		private readonly string _connectionString;

		public SqliteConnection DbConnection { get; set; }
		
		public SqliteDbFixture(string dir) {
			var fileName = typeof(T).Name + ".db";
			var connectionStringBuilder = new SqliteConnectionStringBuilder();
			connectionStringBuilder.Pooling = false; // prevents the db files from being locked
			connectionStringBuilder.DataSource = Path.Combine(dir, fileName);
			_connectionString = connectionStringBuilder.ConnectionString;
		}
		
		public Task InitializeAsync() {
			DbConnection = new SqliteConnection(_connectionString);
			return DbConnection.OpenAsync();
		}

		public Task DisposeAsync() {
			DbConnection.Close();
			DbConnection.Dispose();
			return Task.CompletedTask;
		}
	}
}
