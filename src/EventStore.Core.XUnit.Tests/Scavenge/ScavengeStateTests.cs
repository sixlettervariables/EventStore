using EventStore.Core.LogV2;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// generally the properties we need of the ScavengeState are tested at a higher
	// level. but a couple of fiddly bits are checked in here
	public class ScavengeStateTests {
		[Fact]
		public void pending_changes_are_still_read() {
			var hasher = new HumanReadableHasher();
			var metastreamLookup = new LogV2SystemStreams();
			var sut = new ScavengeStateBuilder(hasher, metastreamLookup).Build();

			var trans = sut.BeginTransaction();

			sut.IncreaseChunkWeight(5, 20);
			sut.IncreaseChunkWeight(6, 40);
			sut.SetMetastreamDiscardPoint("$$ab-1", DiscardPoint.DiscardBefore(20));

			Assert.Equal(60, sut.SumChunkWeights(5, 6));
			Assert.True(sut.TryGetMetastreamDiscardPoint("$$ab-1", out var actual));
			Assert.Equal(DiscardPoint.DiscardBefore(20), actual);
			trans.Commit(new ScavengeCheckpoint.Accumulating(
				new ScavengePoint(default, default, default, default),
				20));
		}
	}
}
