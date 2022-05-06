using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// these tests test that the right steps happen and the right results are obtained when scavenge is
	// run on a database that already has already been scavenged.
	// a new scavenge point may need to be created, but not necessarily.
	public class SubsequentScavengeTests {
		[Fact]
		public async Task can_create_first_scavenge_point() {
			// first scavenge creates the first scavenge point SP-1
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk())
				.AssertTrace(
					Tracer.Line("Accumulating from start to SP-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 0"),
					Tracer.Line("        Checkpoint: Accumulating SP-0 done Chunk 0"),
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
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(0, 2),
						new LogRecord[] { null },
					},
					x => new[] {
						x.Recs[0].KeepIndexes(0, 2),
						// not expecting the new scavengepoint to be indexed because we didn't index it.
						new LogRecord[] { },
					});

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			Assert.IsType<ScavengeCheckpoint.Done>(checkpoint);

			// subsequent scavenge creates another scavenge point SP-2
			(state, _) = await new Scenario()
				.WithTracerFrom(scenario)
				.WithDb(db)
				.WithState(x => x.ExistingState(state))
				.AssertTrace(
					Tracer.Line("Accumulating from SP-0 to SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-1 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-1 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-1 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-1 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-1 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-1"),
					Tracer.Line("Commit"))
				.RunAsync(
					x => new[] {
						x.Recs[0],
						new LogRecord[] { null, null },
					},
					x => new[] {
						x.Recs[0],
						// not expecting the new scavengepoint to be indexed because we didn't index it.
						new LogRecord[] { null },
					});
		}


		[Fact]
		public async Task can_create_subsequent_scavenge_point() {
			// set up some state and some chunks simulating a scavenge that has been completed
			// and then some new records added. it should create a new SP and perform an an incremental
			// scavenge
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						// events 0-4 removed in a previous scavenge
						Rec.Prepare(t++, "ab-1", eventNumber: 5))
					.Chunk(
						ScavengePoint(t++),
						// two new records written since the previous scavenge
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk())
				.MutateState(x => {
					x.SetOriginalStreamMetadata("ab-1", MaxCount1);
					x.SetOriginalStreamDiscardPoints(
						StreamHandle.ForHash<string>(98),
						DiscardPoint.DiscardBefore(5),
						DiscardPoint.DiscardBefore(5));
					x.SetCheckpoint(new ScavengeCheckpoint.Done(new ScavengePoint(
						//qq 1024*1024 is the chunk size, want less magic
						//qq probably refactor
						upToPosition: 1024 * 1024,
						eventNumber: 0,
						effectiveNow: EffectiveNow,
						threshold: 0)));
				})
				.AssertTrace(
					Tracer.Line("Accumulating from SP-0 to SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-1 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 1"),
					Tracer.Line("        Checkpoint: Accumulating SP-1 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-1 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        SetDiscardPoints(98, Discard before 7, Discard before 7)"),
					Tracer.Line("        Checkpoint: Calculating SP-1 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-1 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-1 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-1 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-1"),
					Tracer.Line("Commit"))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1].KeepIndexes(0, 2),
						new LogRecord[] { null },
					},
					x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1].KeepIndexes(0, 2),
						// not expecting the new scavengepoint to be indexed because we didn't index it.
						new LogRecord[] { },
					});
		}

		[Fact]
		public async Task can_find_existing_scavenge_point() {
			// set up some state and some chunks simulating a scavenge that has been completed
			// and then some new records added including a SP. it should perform an an incremental
			// scavenge using that SP.
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						// events 0-4 removed in a previous scavenge
						Rec.Prepare(t++, "ab-1", eventNumber: 5))
					.Chunk(
						ScavengePoint(t++),
						// five new records written since the previous scavenge
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						ScavengePoint(t++), // <-- SP-1 added by another node
						Rec.Prepare(t++, "ab-1"),
						ScavengePoint(t++), // <-- SP-2 added by another node
						Rec.Prepare(t++, "ab-1")))
				.MutateState(x => {
					x.SetOriginalStreamMetadata("ab-1", MaxCount1);
					x.SetOriginalStreamDiscardPoints(
						StreamHandle.ForHash<string>(98),
						DiscardPoint.DiscardBefore(5),
						DiscardPoint.DiscardBefore(5));
					x.SetCheckpoint(new ScavengeCheckpoint.Done(new ScavengePoint(
						//qq 1024*1024 is the chunk size, want less magic
						//qq probably refactor
						upToPosition: 1024 * 1024,
						eventNumber: 0,
						effectiveNow: EffectiveNow,
						threshold: 0)));
				})
				.AssertTrace(
					Tracer.Line("Accumulating from SP-0 to SP-2"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Accumulating SP-2 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Reading Chunk 1"),
					Tracer.Line("        Checkpoint: Accumulating SP-2 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Calculating SP-2"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Calculating SP-2 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Begin"),
					Tracer.Line("        SetDiscardPoints(98, Discard before 7, Discard before 7)"),
					Tracer.Line("        Checkpoint: Calculating SP-2 done Hash: 98"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing chunks for SP-2"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-2 done None"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 0-0"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk0"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-2 done Chunk 0"),
					Tracer.Line("    Commit"),
					Tracer.Line("    Opening Chunk 1-1"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Switched in chunk chunk1"),
					Tracer.Line("        Checkpoint: Executing chunks for SP-2 done Chunk 1"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Executing index for SP-2"),
					Tracer.Line("    Begin"),
					Tracer.Line("        Checkpoint: Executing index for SP-2"),
					Tracer.Line("    Commit"),
					Tracer.Line("Done"),

					Tracer.Line("Begin"),
					Tracer.Line("    Checkpoint: Done SP-2"),
					Tracer.Line("Commit"))
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1].KeepIndexes(0, 2),
						x.Recs[2],
					},
					x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1].KeepIndexes(0, 2),
						x.Recs[2],
					});
		}

		[Fact]
		public async Task can_subsequent_scavenge_without_state() {
			// say we deleted the state, or old scavenge has been run but not new scavenge
			// so there is no state.
			var t = 0;
			var scenario = new Scenario();
			var (state, db) = await scenario
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						// events 0-4 removed in a previous scavenge
						Rec.Prepare(t++, "ab-1", eventNumber: 5))
					.Chunk(
						ScavengePoint(t++),
						// five new records written since the previous scavenge
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"))
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						ScavengePoint(t++), // <-- SP-1 added by another node
						Rec.Prepare(t++, "ab-1"),
						ScavengePoint(t++), // <-- SP-2 added by another node
						Rec.Prepare(t++, "ab-1")))
				.MutateState(x => {
				})
				.RunAsync(
					x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1].KeepIndexes(0, 2),
						x.Recs[2],
					},
					x => new[] {
						x.Recs[0].KeepIndexes(0),
						x.Recs[1].KeepIndexes(0, 2),
						x.Recs[2],
					});
		}
	}

	//qqqqqqqqqq add a test for moving (or attempting to move) the discard points backwards
}
