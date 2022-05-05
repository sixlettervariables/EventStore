using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class SubsequentScavengeTests {
		//qq so lets see whta we want to test here.
		/*
		 * - that each component is working correctly for the next scavenge.
		 *   - we will check the overall effect of the scavenge
		 *   - we will wrap the adapters with something that does logging that we can check.
		 *         
		 * - that starting up the scavenge is working correctly
		 *     [*] 1. first scavenge there is no scavenge point so we create one
		 *     [*] 2. second scavenge there is a scavenge point that is complete so we create one
		 *     - 3. finding a scavenge point that has been created by another node - use that instead of creating a new one.
		 *     
		 *  - that its all working ok including when extra records have been written to the db
		 *    in between. or more time has passed.
		 */

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

		//[Fact]
		//public async Task can_create_subsequent_scavenge_point() {
		//	//qqqq set up some state and some chunks simulating a scavenge that has been completed
		//	// and then some new records added. it should create a new SP and performan an incremental
		//	// scavenge
		//}

		//[Fact]
		//public async Task can_create_find_existing_scavenge_point() {
		//	//qqq set up some state and some chunks simulating a scavenge that has been completed
		//	// and then some new records added and a new scavenge point. it should perform an
		//	// incremental scavenge against that scavenge point
		//}
	}
}
