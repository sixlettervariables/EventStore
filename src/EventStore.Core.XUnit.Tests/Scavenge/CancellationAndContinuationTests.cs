using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class CancellationAndContinuationTests {
		// in these tests we we want to
		// - run a scavenge
		// - have a log record trigger the cancellation of that scavenge at a particular point
		// - check that the checkpoint has been set correctly in the scavenge state
		// - complete the scavenge
		// - check it continued from the checkpoint
		// - check it produced the right results

		[Fact]
		public async Task accumulator_checkpoints_immediately() {
			var t = 0;
			var (state, _) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$cd-cancel-accumulation"))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenAccumulatingMetaRecordFor("cd-cancel-accumulation")
				.AssertTrace(
					Tracer.Line("Accumulating from start to SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 0"),
					Tracer.Line("    Rollback"),
					Tracer.Line("Exception accumulating"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var accumulating = Assert.IsType<ScavengeCheckpoint.Accumulating>(checkpoint);
			Assert.Null(accumulating.DoneLogicalChunkNumber);
		}

		[Fact]
		public async Task calculator_checkpoints_immediately() {
			var t = 0;
			var (state, _) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "cd-cancel-calculation"),
						Rec.Prepare(t++, "$$cd-cancel-calculation", metadata: MaxCount1))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenCalculatingOriginalStream("cd-cancel-calculation")
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
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("    Rollback"),
					Tracer.Line("Exception calculating"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var calculating = Assert.IsType<ScavengeCheckpoint.Calculating<string>>(checkpoint);
			Assert.Equal("None", calculating.DoneStreamHandle.ToString());
		}

		[Fact]
		public async Task chunk_executor_checkpoints_immediately() {
			var t = 0;
			var (state, _) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "cd-cancel-chunk-execution"))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenExecutingChunk("cd-cancel-chunk-execution")
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
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("    Rollback"),
					Tracer.Line("Exception executing chunks"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var executing = Assert.IsType<ScavengeCheckpoint.ExecutingChunks>(checkpoint);
			Assert.Null(executing.DoneLogicalChunkNumber);
		}

		[Fact()]
		public async Task index_executor_checkpoints_immediately() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "cd-cancel-index-execution"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenExecutingIndexEntry("cd-cancel-index-execution")
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
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Exception executing index"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var executing = Assert.IsType<ScavengeCheckpoint.ExecutingIndex>(checkpoint);
		}

		[Fact]
		public async Task can_cancel_during_accumulation_and_resume() {
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount2),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "$$cd-cancel-accumulation"))
					.Chunk(
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenAccumulatingMetaRecordFor("cd-cancel-accumulation")
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
					Tracer.Line("    Rollback"),
					Tracer.Line("Exception accumulating"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var accumulating = Assert.IsType<ScavengeCheckpoint.Accumulating>(checkpoint);
			Assert.Equal(0, accumulating.DoneLogicalChunkNumber);

			// now complete the scavenge
			(state, _) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.AssertTrace(
					// accumulation continues from checkpoint
					Tracer.Line("Accumulating from checkpoint: Accumulating SP-0 done Chunk 0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 1"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 2"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 2"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 3"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 3"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					// the rest is fresh for SP-0
					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        SetDiscardPoints(98, Discard before 1, Discard before 1)"),
					// no discard points to set for cd-cancel-accumulation (hash 100)
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 100"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 100"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 2-2"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk2"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-0"),
					Tracer.Line("Commit"))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 2),
					x.Recs[1].KeepIndexes(0),
					x.Recs[2].KeepIndexes(0),
					x.Recs[3],
				});

			// scavenge completed
			Assert.True(state.TryGetCheckpoint(out checkpoint));
			var done = Assert.IsType<ScavengeCheckpoint.Done>(checkpoint);
			//qq Assert.Equal(thecorrectscavengepoint/number, done.?);
		}

		[Fact]
		public async Task can_cancel_during_calculation_and_resume() {
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cd-cancel-calculation"),
						Rec.Prepare(t++, "$$cd-cancel-calculation", metadata: MaxCount1))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenCalculatingOriginalStream("cd-cancel-calculation")
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
					Tracer.Line("        SetDiscardPoints(98, Discard before 2, Discard before 2)"),
					// throw while calculating 100
					Tracer.Line("    Rollback"),
					Tracer.Line("Exception calculating"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var calculating = Assert.IsType<ScavengeCheckpoint.Calculating<string>>(checkpoint);
			Assert.Equal("None", calculating.DoneStreamHandle.ToString());

			// now complete the scavenge
			(state, _) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.AssertTrace(
					// no accumulation
					// calculation continues from checkpoint
					Tracer.Line("Calculating from checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Begin"),
					//qq the discard points for 98 that we set should have been rolled back
					// so we should set them again here, but the inmem doesn't roll back
					//Tracer.Line("        SetDiscardPoints(98, Discard before 2, Discard before 2)"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 100"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 100"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					// the rest is fresh for SP-0
					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-0"),
					Tracer.Line("Commit"))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0),
					x.Recs[1].KeepIndexes(0, 1, 2),
					x.Recs[2],
				});

			// scavenge completed
			Assert.True(state.TryGetCheckpoint(out checkpoint));
			var done = Assert.IsType<ScavengeCheckpoint.Done>(checkpoint);
		}

		[Fact]
		public async Task can_cancel_during_chunk_execution_and_resume() {
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "$$ab-2", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "cd-cancel-chunk-execution"),
						Rec.Prepare(t++, "ab-2"))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenExecutingChunk("cd-cancel-chunk-execution")
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
					Tracer.Line("        SetDiscardPoints(ab-1, Discard before 1, Discard before 1)"),
					// no discard points to set for ab-2
					Tracer.Line("        Checkpoint: Calculating SP-0 done Id: ab-2"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Id: ab-2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("    Rollback"),
					Tracer.Line("Exception executing chunks"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var executing = Assert.IsType<ScavengeCheckpoint.ExecutingChunks>(checkpoint);
			Assert.Equal(0, executing.DoneLogicalChunkNumber);

			// now complete the scavenge
			(state, _) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.AssertTrace(
					// no accumulation
					// no calculation
					// chunk execution continues from checkpoint
					Tracer.Line("Executing chunks from checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					// the rest is fresh for SP-0
					Tracer.Line("Executing index for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-0"),
					Tracer.Line("Commit"))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1),
					x.Recs[1].KeepIndexes(0, 1, 2),
					x.Recs[2],
				});

			// scavenge completed
			Assert.True(state.TryGetCheckpoint(out checkpoint));
			var done = Assert.IsType<ScavengeCheckpoint.Done>(checkpoint);
		}

		[Fact]
		public async Task can_cancel_during_index_execution_and_resume() {
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "cd-cancel-index-execution"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePoint(t++)))
				.CancelWhenExecutingIndexEntry("cd-cancel-index-execution")
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
					Tracer.Line("        SetDiscardPoints(98, Discard before 3, Discard before 3)"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Exception executing index"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var executing = Assert.IsType<ScavengeCheckpoint.ExecutingIndex>(checkpoint);

			// now complete the scavenge
			(state, _) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				// makes sure we dont reaccumulate
				.CancelWhenAccumulatingMetaRecordFor("ab-1")
				// make sure we dont recalculate
				.CancelWhenCalculatingOriginalStream("ab-1")
				// make sure we dont rescavenge the chunks
				.CancelWhenExecutingChunk("ab-1")
				//qq todo: consider making sure we dont rescavenge a ptable that was scavenged
				// but this is quite awkward, perhaps we can find another way to test it.
				// at least wait until we are not using the scaffold.
				//.CancelWhenExecutingIndexEntry("ab-1")
				.AssertTrace(
					Tracer.Line("Executing index from checkpoint: Executing index for SP-0"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-0"),
					Tracer.Line("Commit"))
				// these are relative to the start of the continued scavenge
				// i.e. we dont expect the continued scavenge to remove any more from the chunks
				// and we expect the index to be brought into line.
				// which is as clear as mud but so far this is the only test that is affected
				.RunAsync(x => new[] {
					x.Recs[0],
					x.Recs[1],
					x.Recs[2],
				});

			// scavenge completed
			Assert.True(state.TryGetCheckpoint(out checkpoint));
			var done = Assert.IsType<ScavengeCheckpoint.Done>(checkpoint);
		}

		//qq cancel and resume for any other stages we might add (merging, tidying)?

		[Fact]
		public async Task can_complete() {
			var t = 0;
			var (state, _) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(ScavengePoint(t++)))
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
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        SetDiscardPoints(98, Discard before 1, Discard before 1)"),
					Tracer.Line("        Checkpoint: Calculating SP-0 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-0 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-0"),
					Tracer.Line("Commit"))
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var executing = Assert.IsType<ScavengeCheckpoint.Done>(checkpoint);
		}
	}
}
