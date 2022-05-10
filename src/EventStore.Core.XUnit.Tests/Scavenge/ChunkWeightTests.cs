using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ChunkWeightTests {
		[Fact]
		public async Task simple() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"), // weight: 2
						Rec.Prepare(t++, "ab-1"), // weight: 2
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(4, state.SumChunkWeights(0, 0));
		}

		[Fact]
		public async Task max_age_maybe_discard() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Expired), // weight: 1
						Rec.Prepare(t++, "ab-1", timestamp: Expired), // weight: 1
						Rec.Prepare(t++, "ab-1", timestamp: Active), // weight: 1
						Rec.Prepare(t++, "ab-1", timestamp: Active), // weight: 0 - last event
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(
						ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(3, state.SumChunkWeights(0, 0));
			Assert.True(state.TryGetOriginalStreamData("ab-1", out var data));
			Assert.Equal(DiscardPoint.DiscardBefore(0), data.DiscardPoint);
			Assert.Equal(DiscardPoint.DiscardBefore(3), data.MaybeDiscardPoint);
		}
	}
}
