using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteScavengeBackendTests : SqliteDbPerTest<SqliteScavengeBackendTests> {
		
		[Fact]
		public void should_successfully_enable_features_on_initialization() {
			var sut = new SqliteScavengeBackend<string>();
			sut.Initialize(Fixture.DbConnection);
			SqliteConnection.ClearAllPools();
		}
	}
}
