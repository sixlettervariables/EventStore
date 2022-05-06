using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// for testing the truncatebefore functionality specifically
	public class TruncateBeforeTests {
		[Fact]
		public async Task simple_truncatebefore() {
			var t = 0;
			await new Scenario().WithDb(x => x
				.Chunk(
					Rec.Prepare(t++, "ab-1"),
					Rec.Prepare(t++, "ab-1"),
					Rec.Prepare(t++, "ab-1"),
					Rec.Prepare(t++, "ab-1"),
					Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: TruncateBefore3))
				.Chunk(ScavengePoint(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(3, 4),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task keep_last_event() {
			var t = 0;
			await new Scenario().WithDb(x => x
				.Chunk(
					Rec.Prepare(t++, "ab-1"),
					Rec.Prepare(t++, "ab-1"),
					Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: TruncateBefore4))
				.Chunk(ScavengePoint(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(1,2),
					x.Recs[1],
				});
		}
	}
}
