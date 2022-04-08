using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class CombinationCriteriaTests : ScavengerTestsBase {
		//qq need more of these, check that the criteria work well in different combinations
		// and set in different orders
		[Fact]
		public void maxcount_then_tombstone() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: MaxCount2),
					Rec.Prepare(1, "ab-1"),
					Rec.Prepare(2, "ab-1"),
					Rec.Delete(3, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(0, 3)
				});
		}

		[Fact]
		public void tombstone_then_maxcount() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "ab-1"),
					Rec.Delete(2, "ab-1"),
					Rec.Prepare(3, "$$ab-1", "$metadata", metadata: MaxCount2))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(2, 3)
				});
		}
	}
}
