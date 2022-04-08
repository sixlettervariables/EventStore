using System;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// for testing functionality that isn't specific to particular discard criteria
	public class MiscelaneousTests : ScavengerTestsBase {
		[Fact]
		public void metadata_first() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: MaxCount1),
					Rec.Prepare(1, "ab-1"),
					Rec.Prepare(2, "ab-1"),
					Rec.Prepare(3, "ab-1"),
					Rec.Prepare(4, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(0, 4)
					});
		}

		[Fact]
		public void nonexistent_stream() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: MaxCount1))
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
					Rec.Prepare(4, "$$ab-1", "$metadata", metadata: MaxCount1),
					Rec.Prepare(5, "$$cd-2", "$metadata", metadata: MaxCount1))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(2, 3, 4, 5)
					});
		}

		[Fact]
		public void metadata_gets_scavenged() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1", "$metadata", metadata: MaxCount1),
					Rec.Prepare(1, "$$ab-1", "$metadata", metadata: MaxCount2))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(1)
					});
		}

		[Fact]
		public void metadata_for_metadata_stream_gets_scavenged() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$$$ab-1", "$metadata", metadata: MaxCount1),
					Rec.Prepare(1, "$$$$ab-1", "$metadata", metadata: MaxCount2))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(1)
					});
		}

		[Fact]
		public void metadata_for_metadata_stream_does_not_apply() {
			// e.g. can't increase the maxcount to three
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$$$ab-1", "$metadata", metadata: MaxCount3),
					Rec.Prepare(1, "$$ab-1"),
					Rec.Prepare(2, "$$ab-1"),
					Rec.Prepare(3, "$$ab-1"),
					Rec.Prepare(4, "$$ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(0, 4)
					});
		}

		[Fact]
		public void metadata_metadata_applies_to_any_type() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "$$ab-1"),
					Rec.Prepare(1, "$$ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0].KeepIndexes(1)
					});
		}

		[Fact]
		public void metadata_in_normal_stream_is_ignored() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1", "$metadata", metadata: MaxCount1),
					Rec.Prepare(1, "ab-1"),
					Rec.Prepare(2, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
						x.Recs[0]
					});
		}

		[Fact]
		public void metadata_in_transaction_not_supported() {
			var e = Assert.Throws<InvalidOperationException>(() => {
				CreateScenario(x => x
					.Chunk(
						Rec.TransSt(0, "$$ab-1"),
						Rec.Prepare(0, "$$ab-1", "$metadata", metadata: MaxCount1))
					.CompleteLastChunk())
					.Run();
			});

			Assert.Equal("Found metadata in transaction in stream $$ab-1", e.Message);
		}
	}
}
