using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class SoftDeleteTests {
		[Fact]
		public async Task undelete_when_soft_delete_across_chunk_boundary() {
			// accumulation has to go up to the scavenge point and not stop at the end of the chunk
			// before, otherwise we could accidentally scavenge the new stream.
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						// stream before deletion
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						// delete
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: SoftDelete),
						// new write that undeletes the stream, but the metadata lands
						// in the next chunk
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: TruncateBefore3),
						ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(4),
						x.Recs[1].KeepIndexes(0, 1),
					});
		}

		//qqqq probably want other tests in here,
		// e.g. that it works correctly being recreated after a cleanup
	}
}
