using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class MaxCountTests : ScavengerTestsBase {
		private static readonly StreamMetadata _meta1 = new StreamMetadata(maxCount: 1);

		[Fact]
		public void simple_maxcount() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "ab-1"),
					Rec.Prepare(2, "ab-1"),
					Rec.Prepare(3, "ab-1"),
					Rec.Prepare(4, "$$ab-1", "$metadata", metadata: _meta1))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(3, 4)
					});
		}

		[Fact]
		public void nonexistent_stream() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: _meta1))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(0)
					});
		}

		[Fact]
		public void multiple_streams() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "cd-2"),
					Rec.Prepare(2, "ab-1"),
					Rec.Prepare(3, "cd-2"),
					Rec.Prepare(4, "$$ab-1", "$metadata", metadata: _meta1),
					Rec.Prepare(5, "$$cd-2", "$metadata", metadata: _meta1))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(2, 3, 4, 5)
					});
		}
	}
}
