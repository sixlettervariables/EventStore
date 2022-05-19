using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// for testing functionality that isn't specific to particular discard criteria
	public class MiscellaneousTests {
		[Fact]
		public async Task metadata_first() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(0, 4),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task nonexistent_stream() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task multiple_streams() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cd-2"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cd-2"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "$$cd-2", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(2, 3, 4, 5),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task metadata_gets_scavenged() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount2))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(1),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task metadata_for_metadata_stream_gets_scavenged() {
			// currently requires a threshold = -1 scavenge to force this.
			// see comments in accumulator.cs
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "$$$$ab-1", "$metadata", metadata: MaxCount2))
					.Chunk(ScavengePointRec(t++, threshold: -1)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(1),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task metadata_for_metadata_stream_does_not_apply() {
			// e.g. can't increase the maxcount to three
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$$$ab-1", "$metadata", metadata: MaxCount3),
						Rec.Prepare(t++, "$$ab-1"),
						Rec.Prepare(t++, "$$ab-1"),
						Rec.Prepare(t++, "$$ab-1"),
						Rec.Prepare(t++, "$$ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(0, 4),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task unusual_metadata_still_takes_effect() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						// this 'metadata' record has a strange type, and does not parse to metadata
						// still, its effect is to reset the metadata of the stream.
						Rec.Prepare(
							transaction: t++,
							stream: "$$ab-1",
							eventType: "sneaky",
							data: BitConverter.GetBytes(0xBAD_1DEA)))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0].KeepIndexes(1, 2, 3),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task metadata_in_normal_stream_is_ignored() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0],
						x.Recs[1],
					});
		}

		[Fact]
		public async Task metadata_in_transaction_not_supported() {
			var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => {
				await new Scenario()
					.WithDb(x => x
						.Chunk(
							Rec.TransSt(0, "$$ab-1"),
							Rec.Prepare(0, "$$ab-1", "$metadata", metadata: MaxCount1))
						.Chunk(ScavengePointRec(1)))
					.RunAsync();
			});

			Assert.Equal("Found metadata in transaction in stream $$ab-1", e.Message);
		}
	}
}
