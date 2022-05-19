using System.Collections.Generic;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteChunkWeightScavengeMapTests : SqliteDbPerTest<SqliteChunkWeightScavengeMapTests> {

		public SqliteChunkWeightScavengeMapTests() : base(deleteDir:false){ //qq Db is locked for some reason and is blocking the deletion.
		}

		[Fact]
		public void can_increase_existing_chunk_weight() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.DbConnection);
			sut.Initialize();
			
			sut[3] = 0.5f;
			
			sut.IncreaseWeight(3, 0.1f);

			Assert.True(sut.TryGetValue(3, out var v1));
			Assert.Equal(0.6f, v1);
		}
		
		[Fact]
		public void can_decrease_existing_chunk_weight() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.DbConnection);
			sut.Initialize();
			
			sut[3] = 0.5f;
			
			sut.IncreaseWeight(3, -0.1f);

			Assert.True(sut.TryGetValue(3, out var v));
			Assert.Equal(0.4f, v);
		}
		
		[Fact]
		public void can_increase_non_existing_chunk_weight() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.DbConnection);
			sut.Initialize();
			
			sut.IncreaseWeight(13, 0.33f);

			Assert.True(sut.TryGetValue(13, out var v));
			Assert.Equal(0.33f, v);
		}

		[Fact]
		public void can_sum_chunk_weights() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.DbConnection);
			sut.Initialize();
			
			sut[0] = 0.1f;
			sut[1] = 0.1f;
			sut[2] = 0.1f;
			sut[3] = 0.1f;
			sut[4] = 0.1f;

			var value = sut.SumChunkWeights(1, 3);
			
			Assert.Equal(0.3f, value);
		}
		
		[Fact]
		public void can_sum_non_existing_chunk_weights() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.DbConnection);
			sut.Initialize();
			
			var value = sut.SumChunkWeights(1, 3);
			
			Assert.Equal(0.0f, value);
		}
		
		[Fact]
		public void can_reset_chunk_weights() {
			var sut = new SqliteChunkWeightScavengeMap(Fixture.DbConnection);
			sut.Initialize();
			
			sut[0] = 0.1f;
			sut[1] = 0.1f;
			sut[2] = 0.1f;
			sut[3] = 0.1f;
			sut[4] = 0.1f;

			sut.ResetChunkWeights(1, 3);
			
			Assert.Collection(sut.AllRecords(),
				item => Assert.Equal(new KeyValuePair<int,float>(0, 0.1f), item),
				item => Assert.Equal(new KeyValuePair<int,float>(4, 0.1f), item));
		}
	}
}
