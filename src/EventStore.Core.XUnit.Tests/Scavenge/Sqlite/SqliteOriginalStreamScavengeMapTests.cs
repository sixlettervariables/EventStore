using System;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteOriginalStreamScavengeMapTests : SqliteDbPerTest<SqliteOriginalStreamScavengeMapTests> {

		public SqliteOriginalStreamScavengeMapTests() : base(deleteDir:false){ //qq Db is locked for some reason and is blocking the deletion.
		}

		[Fact]
		public void can_set_original_stream_data() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var data = new OriginalStreamData {
				DiscardPoint = DiscardPoint.DiscardIncluding(5),
				IsTombstoned = true,
				MaxAge = TimeSpan.FromDays(13),
				MaxCount = 33,
				MaybeDiscardPoint = DiscardPoint.DiscardBefore(long.MaxValue-1),
				TruncateBefore = 43
			};

			sut[33] = data;

			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(data.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_tombstone_of_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var data = new OriginalStreamData {
				DiscardPoint = DiscardPoint.DiscardIncluding(5),
				MaxAge = TimeSpan.FromDays(13),
				MaxCount = 33,
				MaybeDiscardPoint = DiscardPoint.DiscardBefore(12),
				TruncateBefore = 43
			};

			sut[33] = data;
			
			sut.SetTombstone(33);
			data.IsTombstoned = true;
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(data.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_tombstone_of_non_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			sut.SetTombstone(33);
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(new OriginalStreamData {
				IsTombstoned = true,
			}.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_stream_metadata_of_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var data = new OriginalStreamData() {
				DiscardPoint = DiscardPoint.DiscardIncluding(5),
				MaybeDiscardPoint = DiscardPoint.DiscardBefore(12)
			};

			sut[33] = data;

			var metadata = new StreamMetadata(
				maxAge: TimeSpan.FromDays(13),
				maxCount: 33,
				truncateBefore: 43);
			
			sut.SetMetadata(33, metadata);
			data.MaxAge = metadata.MaxAge;
			data.MaxCount = metadata.MaxCount;
			data.TruncateBefore = metadata.TruncateBefore;
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(data.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_stream_metadata_without_max_age() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var metadata = new StreamMetadata(
				maxCount: 33,
				truncateBefore: 43);
			
			sut.SetMetadata(33, metadata);
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(new OriginalStreamData() {
				MaxCount = metadata.MaxCount,
				TruncateBefore = metadata.TruncateBefore
			}.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_stream_metadata_without_max_count() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var metadata = new StreamMetadata(
				maxAge: TimeSpan.FromDays(13),
				truncateBefore: 43);
			
			sut.SetMetadata(33, metadata);
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(new OriginalStreamData() {
				MaxAge = metadata.MaxAge,
				TruncateBefore = metadata.TruncateBefore
			}.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_stream_metadata_without_truncating() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var metadata = new StreamMetadata(
				maxAge: TimeSpan.FromDays(13),
				maxCount: 33);
			
			sut.SetMetadata(33, metadata);
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(new OriginalStreamData() {
				MaxAge = metadata.MaxAge,
				MaxCount = metadata.MaxCount
			}.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_stream_metadata_of_non_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var metadata = new StreamMetadata(
				maxAge: TimeSpan.FromDays(13),
				maxCount: 33,
				truncateBefore: 43);
			
			sut.SetMetadata(33, metadata);
			
			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(new OriginalStreamData() {
				MaxAge = metadata.MaxAge,
				MaxCount = metadata.MaxCount,
				TruncateBefore = metadata.TruncateBefore
			}.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_discard_points_of_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var data = new OriginalStreamData() {
				MaxAge = TimeSpan.FromDays(13),
				MaxCount = 33,
				TruncateBefore = 43
			};

			sut[33] = data;

			data.DiscardPoint = DiscardPoint.DiscardIncluding(5);
			data.MaybeDiscardPoint = DiscardPoint.DiscardIncluding(12);
			sut.SetDiscardPoints(33, data.DiscardPoint, data.MaybeDiscardPoint);

			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(data.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_set_discard_points_of_non_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var discardPoint = DiscardPoint.DiscardIncluding(5);
			var maybeDiscardPoint = DiscardPoint.DiscardIncluding(12);
			sut.SetDiscardPoints(33, discardPoint, maybeDiscardPoint);

			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(new OriginalStreamData() {
				DiscardPoint = discardPoint,
				MaybeDiscardPoint = maybeDiscardPoint,
			}.ToString(), v.ToString());
		}
		
		[Fact]
		public void can_get_stream_execution_details() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var data = new OriginalStreamData() {
				DiscardPoint = DiscardPoint.DiscardIncluding(5),
				MaybeDiscardPoint = DiscardPoint.DiscardIncluding(43),
				MaxAge = TimeSpan.FromDays(13)
			};

			sut[33] = data;

			Assert.True(sut.TryGetStreamExecutionDetails(33, out var v));
			Assert.Equal(new StreamExecutionDetails(data.DiscardPoint, data.MaybeDiscardPoint, data.MaxAge), v);
		}
		
		[Fact]
		public void can_try_get_stream_execution_details_of_non_existing() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			Assert.False(sut.TryGetStreamExecutionDetails(33, out var v));
			Assert.Equal(default, v);
		}
		
		[Fact]
		public void can_enumerate_all_items() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var osd = GetOriginalStreamTestData();
			
			sut[0] = osd[0];
			sut[1] = osd[1];
			sut[2] = osd[2];
			sut[3] = osd[3];
			sut[4] = osd[4];

			Assert.Collection(sut,
				item => {
					Assert.Equal(0, item.Key);
					Assert.Equal(osd[0].ToString(), item.Value.ToString());
				},
				item => {
					Assert.Equal(1, item.Key);
					Assert.Equal(osd[1].ToString(), item.Value.ToString());
				},
				item => {
					Assert.Equal(2, item.Key);
					Assert.Equal(osd[2].ToString(), item.Value.ToString());
				},
				item => {
					Assert.Equal(3, item.Key);
					Assert.Equal(osd[3].ToString(), item.Value.ToString());
				},
				item => {
					Assert.Equal(4, item.Key);
					Assert.Equal(osd[4].ToString(), item.Value.ToString());
				});
		}

		[Fact]
		public void can_enumerate_from_checkpoint() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var osd = GetOriginalStreamTestData();
			
			sut[0] = osd[0];
			sut[1] = osd[1];
			sut[2] = osd[2];
			sut[3] = osd[3];
			sut[4] = osd[4];
			
			Assert.Collection(sut.FromCheckpoint(2),
				item => {
					Assert.Equal(3, item.Key);
					Assert.Equal(osd[3].ToString(), item.Value.ToString());
				},
				item => {
					Assert.Equal(4, item.Key);
					Assert.Equal(osd[4].ToString(), item.Value.ToString());
				});
		}
		
		[Fact]
		public void can_remove_value_from_map() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			var osd = GetOriginalStreamTestData();
			
			sut[33] = osd[0];
			sut[1] = osd[1];

			Assert.True(sut.TryGetValue(33, out _));
			Assert.True(sut.TryRemove(33, out var removedValue));
			Assert.Equal(osd[0].ToString(), removedValue.ToString());
			Assert.False(sut.TryGetValue(33, out _));
			
			Assert.True(sut.TryGetValue(1, out var v));
			Assert.Equal(osd[1].ToString(), v.ToString());
		}
		
		[Fact]
		public void can_try_remove_value_from_map() {
			var sut = new SqliteOriginalStreamScavengeMap<int>("OriginalStreamScavengeMap", Fixture.DbConnection);
			sut.Initialize();

			Assert.False(sut.TryRemove(33, out _));
		}

		private OriginalStreamData[] GetOriginalStreamTestData() {
			return new[] {
				new OriginalStreamData() {
					IsTombstoned = false,
					MaxAge = TimeSpan.FromDays(3),
					MaxCount = 10,
					TruncateBefore = 1
				},
				new OriginalStreamData() {
					DiscardPoint = DiscardPoint.DiscardBefore(10),
					IsTombstoned = true,
					MaxAge = TimeSpan.FromDays(3),
					MaxCount = 100,
					TruncateBefore = 13,
					MaybeDiscardPoint = DiscardPoint.DiscardBefore(21)
				},
				new OriginalStreamData() {
					DiscardPoint = DiscardPoint.DiscardBefore(20),
					MaxAge = TimeSpan.FromHours(12),
					MaybeDiscardPoint = DiscardPoint.DiscardBefore(21)
				},
				new OriginalStreamData() {
					DiscardPoint = DiscardPoint.DiscardBefore(10),
					IsTombstoned = true,
					MaxAge = TimeSpan.FromDays(33),
					TruncateBefore = 11,
				},
				new OriginalStreamData() {
					DiscardPoint = DiscardPoint.DiscardBefore(300),
					MaxAge = TimeSpan.FromDays(300),
					MaxCount = 1000,
					TruncateBefore = 1333,
					MaybeDiscardPoint = DiscardPoint.DiscardBefore(500)
				}
			};
		}
	}
}
