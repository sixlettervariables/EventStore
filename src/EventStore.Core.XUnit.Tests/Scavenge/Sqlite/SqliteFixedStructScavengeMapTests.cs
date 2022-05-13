using System.Collections.Generic;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite
{
	public class SqliteFixedStructScavengeMapTests : SqliteDbPerTest<SqliteFixedStructScavengeMapTests> {

		public SqliteFixedStructScavengeMapTests() : base(deleteDir:false){ //qq Db is locked for some reason and is blocking the deletion.
		}

		[Fact]
		public void can_use_fixed_struct_value_type_map() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("FixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));
			
			sut[22] = DiscardPoint.DiscardBefore(22);
			sut[33] = DiscardPoint.DiscardBefore(33);

			Assert.True(sut.TryGetValue(22, out var v1));
			Assert.Equal(DiscardPoint.DiscardBefore(22), v1);
			
			Assert.True(sut.TryGetValue(33, out var v2));
			Assert.Equal(DiscardPoint.DiscardBefore(33), v2);
		}
		
		[Fact]
		public void can_overwrite_value() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("OverwriteValueFixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));
			
			sut[33] = DiscardPoint.DiscardBefore(22);
			sut[33] = DiscardPoint.DiscardBefore(33);

			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(DiscardPoint.DiscardBefore(33), v);
		}
		
		[Fact]
		public void can_enumerate_all_items() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("EnumerateFixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			sut[0] = DiscardPoint.DiscardBefore(10);
			sut[1] = DiscardPoint.DiscardBefore(20);
			sut[2] = DiscardPoint.DiscardBefore(30);
			sut[3] = DiscardPoint.DiscardBefore(40);
			sut[4] = DiscardPoint.DiscardBefore(50);
			
			Assert.Collection(sut.AllRecords(),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(0, DiscardPoint.DiscardBefore(10)), item),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(1, DiscardPoint.DiscardBefore(20)), item),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(2, DiscardPoint.DiscardBefore(30)), item),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(3, DiscardPoint.DiscardBefore(40)), item),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(4, DiscardPoint.DiscardBefore(50)), item));
		}
		
		[Fact]
		public void can_enumerate_from_checkpoint() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("EnumerateFixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			sut[0] = DiscardPoint.DiscardBefore(10);
			sut[1] = DiscardPoint.DiscardBefore(20);
			sut[2] = DiscardPoint.DiscardBefore(30);
			sut[3] = DiscardPoint.DiscardBefore(40);
			sut[4] = DiscardPoint.DiscardBefore(50);
			
			Assert.Collection(sut.ActiveRecordsFromCheckpoint(2),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(3, DiscardPoint.DiscardBefore(40)), item),
				item => Assert.Equal(new KeyValuePair<int,DiscardPoint>(4, DiscardPoint.DiscardBefore(50)), item));
		}
		
		[Fact]
		public void can_try_get_value_of_non_existing() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("TryGetValueOfFixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			Assert.False(sut.TryGetValue(33, out var v));
			Assert.Equal(default, v);
		}
		
		[Fact]
		public void can_remove_value() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("TryGetValueOfFixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			sut[0] = DiscardPoint.DiscardBefore(10);
			
			Assert.True(sut.TryRemove(0, out var v));
			Assert.Equal(DiscardPoint.DiscardBefore(10), v);
		}
		
		[Fact]
		public void can_try_remove_value() {
			var sut = new SqliteFixedStructScavengeMap<int, DiscardPoint>("TryGetValueOfFixedStructMap");
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			Assert.False(sut.TryRemove(0, out var v));
			Assert.Equal(default, v);
		}
	}
}
