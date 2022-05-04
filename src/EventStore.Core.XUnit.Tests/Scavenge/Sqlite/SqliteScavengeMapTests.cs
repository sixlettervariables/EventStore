using System;
using System.Collections.Generic;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite
{
	public class SqliteScavengeMapTests : SqliteDbPerTest<SqliteScavengeMapTests> {

		public SqliteScavengeMapTests() : base(deleteDir:false){ //qq Db is locked for some reason and is blocking the deletion.
		}
		
		[Fact]
		public void throws_on_unsupported_type() {
			Assert.Throws<ArgumentException>(
				() => new SqliteScavengeMap<byte, int>("UnsupportedKeyTypeMap", Fixture.DbConnection));
			
			Assert.Throws<ArgumentException>(
				() => new SqliteScavengeMap<int, byte>("UnsupportedValueTypeMap", Fixture.DbConnection));
		}
		
		[Fact]
		public void initializes_table_only_once() {
			var sut = new SqliteScavengeMap<int, int>("SomeMap", Fixture.DbConnection);

			sut.Initialize();
			sut[33] = 1;
			
			Assert.Null(Record.Exception(sut.Initialize));
			Assert.True(sut.TryGetValue(33, out var value));
			Assert.Equal(1, value);
		}
		
		[Fact]
		public void can_use_int_float_map() {
			var sut = new SqliteScavengeMap<int, float>("IntFloatMap", Fixture.DbConnection);
			sut.Initialize();

			sut[33] = 1;
			sut[1] = 33;
			
			Assert.True(sut.TryGetValue(33, out var v1));
			Assert.Equal(1, v1);
			
			Assert.True(sut.TryGetValue(1, out var v2));
			Assert.Equal(33, v2);
		}

		[Fact]
		public void can_use_string_map() {
			var sut = new SqliteScavengeMap<string, string>("StringMap", Fixture.DbConnection);
			sut.Initialize();

			sut["string"] = "string";
			
			Assert.True(sut.TryGetValue("string", out var v));
			Assert.Equal("string", v);
		}
		
		[Fact]
		public void can_not_add_the_same_key_twice() {
			var sut = new SqliteScavengeMap<int, float>("DuplicateKeyMap", Fixture.DbConnection);
			sut.Initialize();

			sut[33] = 1;

			Assert.Throws<ArgumentException>(() => sut[33] = 1);
		}
		
		[Fact]
		public void can_store_max_unsigned_long() {
			var sut = new SqliteScavengeMap<ulong, ulong>("UnsignedLongMaxValueMap", Fixture.DbConnection);
			sut.Initialize();

			sut[ulong.MaxValue] = ulong.MaxValue;
			
			Assert.True(sut.TryGetValue(ulong.MaxValue, out var v));
			Assert.Equal(ulong.MaxValue, v);
		}
		
		[Fact]
		public void can_remove_value_from_map() {
			var sut = new SqliteScavengeMap<int, int>("RemoveValueMap", Fixture.DbConnection);
			sut.Initialize();

			sut[33] = 1;
			sut[1] = 33;

			Assert.True(sut.TryGetValue(33, out _));
			Assert.True(sut.TryRemove(33, out var removedValue));
			Assert.Equal(1, removedValue);
			Assert.False(sut.TryGetValue(33, out _));
			
			Assert.True(sut.TryGetValue(1, out var v));
			Assert.Equal(33, v);
		}
		
		[Fact]
		public void can_try_remove_value_from_map() {
			var sut = new SqliteScavengeMap<int, int>("TryRemoveValueMap", Fixture.DbConnection);
			sut.Initialize();

			Assert.False(sut.TryRemove(33, out _));
		}
		
		[Fact]
		public void can_enumerate_map() {
			var sut = new SqliteScavengeMap<int, int>("EnumerateMap", Fixture.DbConnection);
			sut.Initialize();

			sut[0] = 4;
			sut[1] = 3;
			sut[2] = 2;
			sut[3] = 1;
			sut[4] = 0;
			
			Assert.Collection(sut,
				item => Assert.Equal(new KeyValuePair<int,int>(0,4), item),
				item => Assert.Equal(new KeyValuePair<int,int>(1,3), item),
				item => Assert.Equal(new KeyValuePair<int,int>(2,2), item),
				item => Assert.Equal(new KeyValuePair<int,int>(3,1), item),
				item => Assert.Equal(new KeyValuePair<int,int>(4,0), item));
		}
		
		[Fact]
		public void can_enumerate_map_from_checkpoint() {
			var sut = new SqliteScavengeMap<int, int>("EnumerateFromCheckpointMap", Fixture.DbConnection);
			sut.Initialize();

			sut[0] = 4;
			sut[1] = 3;
			sut[2] = 2;
			sut[3] = 1;
			sut[4] = 0;
			
			Assert.Collection(sut.FromCheckpoint(2),
				item => Assert.Equal(new KeyValuePair<int,int>(3,1), item),
				item => Assert.Equal(new KeyValuePair<int,int>(4,0), item));
		}
	}
}
