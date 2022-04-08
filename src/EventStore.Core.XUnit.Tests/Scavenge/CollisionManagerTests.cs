using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// generally the properties we need of the CollisionManager are tested at a higher
	// level. but a couple of fiddly bits are checked in here
	public class CollisionManagerTests {
		[Fact]
		public void foo_attempt_at_originalstreamdatas() {
			var hasher = new HumanReadableHasher();
			var collisions = new InMemoryScavengeMap<string, Unit>();

			var sut = GenSut(hasher, collisions);

			// add a non-collision for a-1
			sut["a-1"] = "foo";

			// value can be retrieved
			Assert.True(sut.TryGetValue("a-1", out var actual));
			Assert.Equal("foo", actual);

			// "a-1" collides with "a-2".
			collisions["a-1"] = Unit.Instance;
			collisions["a-2"] = Unit.Instance;
			sut.NotifyCollision("a-1");

			// value for "a-1" can still be retrieved
			Assert.True(sut.TryGetValue("a-1", out actual));
			Assert.Equal("foo", actual);

			// "a-2" not retrieveable since we never set it
			Assert.False(sut.TryGetValue("a-2", out _));
		}

		private static CollisionMap<string, string> GenSut(
			ILongHasher<string> hasher,
			IScavengeMap<string, Unit> collisions) {

			var sut = new CollisionMap<string, string>(
				hasher,
				x => collisions.TryGetValue(x, out _),
				new InMemoryScavengeMap<ulong, string>(),
				new InMemoryScavengeMap<string, string>());

			return sut;
		}
	}
}
