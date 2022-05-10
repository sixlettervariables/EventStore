using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// for testing the maxage functionality specifically
	public class MaxAgeTests {
		[Fact]
		public async Task simple_maxage() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Active),
						Rec.Prepare(t++, "ab-1", timestamp: Active),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3, 4),
						x.Recs[1],
					},
					x => new[] {
						x.Recs[0],
						x.Recs[1],
					});
		}

		[Fact]
		public async Task keep_last_event() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3),
						x.Recs[1],
					},
					x => new[] {
						x.Recs[0],
						x.Recs[1],
					});
		}

		[Fact]
		public async Task whole_chunk_expired() {
			// the records can be removed from the chunks and the index
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired))
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Active),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(),
						x.Recs[1].KeepIndexes(0, 1),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task whole_chunk_expired_keep_last_event() {
			// the records can be removed from the chunks and the index
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired),
						Rec.Prepare(t++, "ab-1", timestamp: Expired))
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2),
						x.Recs[1].KeepIndexes(0),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task whole_chunk_active() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1", timestamp: Active),
						Rec.Prepare(t++, "ab-1", timestamp: Active),
						Rec.Prepare(t++, "ab-1", timestamp: Active))
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0],
						x.Recs[1],
						x.Recs[2],
					});
		}
	}
}
