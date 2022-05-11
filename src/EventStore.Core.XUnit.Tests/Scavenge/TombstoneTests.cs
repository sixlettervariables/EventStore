using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TombstoneTests {
		//qqq we will probably want a set of these to test out collisions when there are tombstones
		// and a set for collisions when there is metadata (maybe combination metadata and tombstone)
		[Fact]
		public async Task simple_tombstone() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "ab-1"),
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(1),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task single_tombstone() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task tombstone_with_metadata() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(t++, "$$ab-1", metadata: MaxCount1),
						Rec.Prepare(t++, "ab-1"),
						Rec.Prepare(t++, "ab-1"),
						Rec.Delete(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					// when the stream is hard deleted we can get rid of _all_ the metadata too
					// do not keep the last metadata record
					x.Recs[0].KeepIndexes(3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task tombstone_in_metadata_stream_not_supported() {
			// eventstore refuses denies access to write such a tombstone in the first place,
			// including in ESv5
			var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => {
				var t = 0;
				await new Scenario()
					.WithDb(x => x
						.Chunk(
							Rec.Delete(t++, "$$ab-1"))
						.Chunk(ScavengePointRec(t++)))
					.RunAsync();
			});

			Assert.Equal("Found Tombstone in metadata stream $$ab-1", e.Message);
		}

		[Fact]
		public async Task tombstone_in_transaction_not_supported() {
			// if we wanted to support this we would have to apply the tombstone at the point that it
			// gets committed. also chunkexecutor would have to be careful not to discard the tombstone
			// of a tombstoned stream even though it can discard pretty much everything in transactions
			// in that case
			var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => {
				await new Scenario()
					.WithDb(x => x
						.Chunk(
							Rec.TransSt(0, "ab-1"),
							Rec.Delete(0, "ab-1"))
						.Chunk(ScavengePointRec(1)))
					.RunAsync();
			});

			Assert.Equal("Found Tombstone in transaction in stream ab-1", e.Message);
		}
	}
}
