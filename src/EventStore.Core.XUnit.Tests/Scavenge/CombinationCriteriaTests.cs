using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class CombinationCriteriaTests {
		//qq need more of these, check that the criteria work well in different combinations
		// and set in different orders
		[Fact]
		public async Task maxcount_then_tombstone() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount2),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(3),
					x.Recs[1],
				});
		}

		//qq pretty much of the tests will start with a scavengepoint already in the database
		//qq add a set of tests that are variations on this - not present, multiple present,
		// last one before extra records, et.
	}
}
