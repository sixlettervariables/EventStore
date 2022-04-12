using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteChunkWeightScavengeMapTests : DirectoryPerTest<SqliteChunkWeightScavengeMapTests> {

		public SqliteChunkWeightScavengeMapTests() : base(deleteDir:false){ //qq Db is locked for some reason and is blocking the deletion.
		}

		[Fact]
		public void can_increase_existing_chunk_weight() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.Directory);
			sut.Initialize();
			
			sut[3] = 0.5f;
			
			sut.IncreaseWeight(3, 0.1f);

			Assert.True(sut.TryGetValue(3, out var v1));
			Assert.Equal(0.6f, v1);
		}
		
		[Fact]
		public void can_decrease_existing_chunk_weight() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.Directory);
			sut.Initialize();
			
			sut[3] = 0.5f;
			
			sut.IncreaseWeight(3, -0.1f);

			Assert.True(sut.TryGetValue(3, out var v));
			Assert.Equal(0.4f, v);
		}
		
		[Fact]
		public void can_increase_non_existing_chunk_weight() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.Directory);
			sut.Initialize();
			
			sut.IncreaseWeight(13, 0.33f);

			Assert.True(sut.TryGetValue(13, out var v));
			Assert.Equal(0.33f, v);
		}
	}
}
