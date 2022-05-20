using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class SoftDeleteTests {
		[Fact]
		public async Task undelete_when_soft_delete_across_chunk_boundary() {
			// accumulation has to go up to the scavenge point and not stop at the end of the chunk
			// before, otherwise we could accidentally scavenge the new stream.
			// the important point is that the scavenge point cant 'appear' to be between the
			// new records and the new metadata. this could cause problems any time
			// the scavenge point operates as if it were in the middle of events supposed to bewritten
			// transactionally.
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

		[Fact]
		public async Task simple_soft_delete() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: SoftDelete))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3), // keep the last event
						x.Recs[1],
					});

			Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			Assert.False(state.TryGetMetastreamData("$$ab-1", out _));
		}

		[Fact]
		public async Task soft_delete_and_recreate() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: SoftDelete),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: TruncateBefore3))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(4, 5),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task can_recreate_soft_deleted_stream_after_scavenge() {
			var t = 0;

			// SP-0 scavenge
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						// stream before deletion
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						// delete
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: SoftDelete))
					.Chunk(ScavengePointRec(t++)) // SP-0
					.Chunk(
						// recreate
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: TruncateBefore3))
					.Chunk(ScavengePointRec(t++))) // SP-1
				.MutateState(x => {
					// make it start with SP-0
					x.SetCheckpoint(new ScavengeCheckpoint.Accumulating(
						ScavengePoint(
							chunk: 1,
							eventNumber: 0),
						doneLogicalChunkNumber: null));
				})
				.AssertTrace(
					Tracer.Line("Accumulating from checkpoint: Accumulating SP-0 done None"),
					Tracer.AnythingElse)
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3),
						x.Recs[1],
						x.Recs[2],
						x.Recs[3],
					});

			Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			Assert.False(state.TryGetMetastreamData("$$ab-1", out _));

			// SP-1 scavenge
			(state, db) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.AssertTrace(
					Tracer.Line("Accumulating from SP-0 to SP-1"),
					Tracer.AnythingElse)
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(),
					x.Recs[1],
					x.Recs[2].KeepIndexes(0, 1),
					x.Recs[3],
				});
		}

		[Fact]
		public async Task can_soft_delete_recreate_and_hard_delete() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						// soft delete
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: SoftDelete),
						// recreate
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: TruncateBefore3),
						// hard delete
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(6),
						x.Recs[1],
					});
		}

		[Fact]
		public async Task can_soft_delete_and_hard_delete() {
			// this might not actually be supported by the database
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						// soft delete
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: SoftDelete),
						// hard delete
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(4),
						x.Recs[1],
					});
		}
	}
}
