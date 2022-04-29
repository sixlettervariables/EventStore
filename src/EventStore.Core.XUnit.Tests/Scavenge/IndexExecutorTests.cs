using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// these systemtically exercise the cases in the IndexExecutor
	// we still do so by testing high level scavenge cases because we are well geared up
	// for that and testing the IndexExeecutor directly would involve more mocks than it is worth.
	public class IndexExecutorTests {
		[Fact]
		public async Task nothing_to_scavenge() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePoint(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1, 2),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task simple_scavenge() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount2))
					.Chunk(ScavengePoint(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(1, 2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task with_collision() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cb-2"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount2))
					.Chunk(ScavengePoint(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(1, 2, 3, 4),
					x.Recs[1],
				});
		}
	}
}
