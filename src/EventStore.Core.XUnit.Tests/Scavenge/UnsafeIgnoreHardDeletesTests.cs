using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class UnsafeIgnoreHardDeletesTests {
		//qq UNSAFE IGNORE HARD DELETES
		// - it would mean the scavenge would have to make sure it removed all the records from all the
		//   chunks and the index (i.e. threshold 0, or 1, //qq and never keep unscavenged if smaller
		// - also it goes without saying that bad things will happen if they mix chunks in from
		//   a node that has not had the scavenge that removed the events

		[Fact]
		public async Task simple_tombstone() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithUnsafeIgnoreHardDeletes()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(),
					x.Recs[1],
				});

			Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			Assert.False(state.TryGetMetastreamData("$$ab-1", out _));
		}
	}
}
