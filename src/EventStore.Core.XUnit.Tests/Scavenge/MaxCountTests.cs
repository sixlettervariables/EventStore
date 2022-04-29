using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// for testing the maxcount functionality specifically
	public class MaxCountTests {
		[Fact]
		public async Task simple_maxcount() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePoint(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(3, 4),
					x.Recs[1],
				});
		}
	}
}
