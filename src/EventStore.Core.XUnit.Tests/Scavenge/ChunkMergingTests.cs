using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ChunkMergingTests {
		[Fact]
		public async Task can_merge() {
			var t = 0;
			await new Scenario()
				.WithMergeChunks(true)
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cd-2")) // keep
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"), // keep
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1)) // keep
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new LogRecord[][] {
						new[] {
							x.Recs[0][1],
							x.Recs[1][1],
							x.Recs[1][2],
							//qq this will change when we switch away from scaffold, the scavengepoint
							// wont be part of the merged chunk
							x.Recs[2][0],
						},
						Array.Empty<LogRecord>(),
						Array.Empty<LogRecord>(),
					},
					x => null);
		}

		[Fact]
		public async Task can_not_merge() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cd-2"))
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					// chunks not merged
					x.Recs[0].KeepIndexes(1),
					x.Recs[1].KeepIndexes(1, 2),
					x.Recs[2].KeepIndexes(0),
				});
		}
	}
}
