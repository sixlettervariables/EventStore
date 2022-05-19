using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ThresholdTests {
		[Fact]
		public async Task negative_threshold_executes_all_chunks() {
			var threshold = -1;
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					// chunk 0: weight 2
					.Chunk(
						Rec.Prepare(t++, "ab-1"))
					// chunk 1: weight 4
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					// chunk 2: weight 0
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						ScavengePointRec(t++, threshold: threshold))
					.CompleteLastChunk())
				.AssertTrace(
					Tracer.Line("Accumulating from start to SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 0"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 1"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 2"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        SetDiscardPoints(98, Active, Discard before 3, Discard before 3)"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"), // executed
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),

					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"), // executed
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),

					Tracer.Line("    Opening Chunk 2-2"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk2"), // executed
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.AnythingElse)
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(), // executed
						x.Recs[1].KeepIndexes(), // executed
						x.Recs[2], // executed
					},
					x => new[] {
						x.Recs[0].KeepIndexes(),
						x.Recs[1].KeepIndexes(),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task zero_threshold_executes_all_chunks_with_positive_weight() {
			var threshold = 0;
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					// chunk 0: weight 2
					.Chunk(
						Rec.Prepare(t++, "ab-1"))
					// chunk 1: weight 4
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					// chunk 2: weight 0
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						ScavengePointRec(t++, threshold: threshold))
					.CompleteLastChunk())
				.AssertTrace(
					Tracer.Line("Accumulating from start to SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 0"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 1"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 2"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        SetDiscardPoints(98, Active, Discard before 3, Discard before 3)"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"), // executed
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),

					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"), // executed
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),

					Tracer.Line("    Opening Chunk 2-2"),
					Tracer.Line("    Begin"),
					//                   no switch, not executed.
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.AnythingElse)
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(), // executed
						x.Recs[1].KeepIndexes(), // executed
						x.Recs[2], // not executed
					},
					x => new[] {
						x.Recs[0].KeepIndexes(),
						x.Recs[1].KeepIndexes(),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task positive_threshold_executes_all_chunks_that_exceed_it() {
			var threshold = 2;
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					// chunk 0: weight 2
					.Chunk(
						Rec.Prepare(t++, "ab-1"))
					// chunk 1: weight 4
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					// chunk 2: weight 0
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						ScavengePointRec(t++, threshold: threshold))
					.CompleteLastChunk())
				.RunAsync(
					x => new[] {
						x.Recs[0], // not executed so still has its records
						x.Recs[1].KeepIndexes(), // executed
						x.Recs[2], // not executed
					},
					x => new[] {
						x.Recs[0].KeepIndexes(),
						x.Recs[1].KeepIndexes(),
						x.Recs[2],
					});
		}
	}
}
