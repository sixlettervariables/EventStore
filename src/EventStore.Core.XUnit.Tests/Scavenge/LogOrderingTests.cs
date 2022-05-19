using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class LogDisorderingTests {
		// if a metadata was ever written with the wrong event number (e.g. 0) due to old bugs
		// the rest of the system will not respect it, so scavenge must not either
		//qqq note that since the index does not necessarily read the out of order metadata, it cannot
		// be reliably scavenged - we will want another database tool to spot and remove such records
		[Fact]
		public async Task wrong_order_metadata_does_not_apply() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1"))
					.Chunk(Rec.Prepare(t++, "ab-1"))
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 5, metadata: MaxCount2))
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0],
						x.Recs[1],
						x.Recs[2],
						x.Recs[3].KeepNone(),
						x.Recs[4],
					});;
		}

		[Fact]
		public async Task wrong_order_metadata_does_not_apply_a() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1")) // 0
					.Chunk(Rec.Prepare(t++, "ab-1")) // 1
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount2)) // 2 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount1)) // 3 skip
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount1)) // 4 skip
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(0, state.SumChunkWeights(0, 0));
			Assert.Equal(0, state.SumChunkWeights(1, 1));
			Assert.Equal(0, state.SumChunkWeights(2, 2));
			Assert.Equal(2, state.SumChunkWeights(3, 3));
			Assert.Equal(2, state.SumChunkWeights(4, 4));

			Assert.True(state.TryGetOriginalStreamData("ab-1", out var originalStreamData));
			Assert.True(state.TryGetMetastreamData("$$ab-1", out var metastreamData));
			
			Assert.Equal(DiscardPoint.KeepAll, originalStreamData.DiscardPoint);
			Assert.Equal(DiscardPoint.KeepAll, metastreamData.DiscardPoint);
		}

		[Fact]
		public async Task wrong_order_metadata_does_not_apply_b() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1")) // 0
					.Chunk(Rec.Prepare(t++, "ab-1")) // 1
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 3, metadata: MaxCount1)) // 2 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 4, metadata: MaxCount2)) // 3 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 4, metadata: MaxCount1)) // 4 skip
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(0, state.SumChunkWeights(0, 0));
			Assert.Equal(0, state.SumChunkWeights(1, 1));
			Assert.Equal(2, state.SumChunkWeights(2, 2));
			Assert.Equal(0, state.SumChunkWeights(3, 3));
			Assert.Equal(2, state.SumChunkWeights(4, 4));

			Assert.True(state.TryGetOriginalStreamData("ab-1", out var originalStreamData));
			Assert.True(state.TryGetMetastreamData("$$ab-1", out var metastreamData));

			Assert.Equal(DiscardPoint.KeepAll, originalStreamData.DiscardPoint);
			Assert.Equal(DiscardPoint.DiscardBefore(4), metastreamData.DiscardPoint);
		}

		[Fact]
		public async Task wrong_order_metadata_does_not_apply_c() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1")) // 0
					.Chunk(Rec.Prepare(t++, "ab-1")) // 1
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 3, metadata: MaxCount1)) // 2 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 4, metadata: MaxCount2)) // 3 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount1)) // 4 skip
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(0, state.SumChunkWeights(0, 0));
			Assert.Equal(0, state.SumChunkWeights(1, 1));
			Assert.Equal(2, state.SumChunkWeights(2, 2));
			Assert.Equal(0, state.SumChunkWeights(3, 3));
			Assert.Equal(2, state.SumChunkWeights(4, 4));

			Assert.True(state.TryGetOriginalStreamData("ab-1", out var originalStreamData));
			Assert.True(state.TryGetMetastreamData("$$ab-1", out var metastreamData));

			Assert.Equal(DiscardPoint.KeepAll, originalStreamData.DiscardPoint);
			Assert.Equal(DiscardPoint.DiscardBefore(4), metastreamData.DiscardPoint);
		}

		[Fact]
		public async Task wrong_order_metadata_does_not_apply_d() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1")) // 0
					.Chunk(Rec.Prepare(t++, "ab-1")) // 1
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 4, metadata: MaxCount2)) // 2 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 2, metadata: MaxCount1)) // 3 skip
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount1)) // 4 skip
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(0, state.SumChunkWeights(0, 0));
			Assert.Equal(0, state.SumChunkWeights(1, 1));
			Assert.Equal(0, state.SumChunkWeights(2, 2));
			Assert.Equal(2, state.SumChunkWeights(3, 3));
			Assert.Equal(2, state.SumChunkWeights(4, 4));

			Assert.True(state.TryGetOriginalStreamData("ab-1", out var originalStreamData));
			Assert.True(state.TryGetMetastreamData("$$ab-1", out var metastreamData));

			Assert.Equal(DiscardPoint.KeepAll, originalStreamData.DiscardPoint);
			Assert.Equal(DiscardPoint.DiscardBefore(4), metastreamData.DiscardPoint);
		}

		[Fact]
		public async Task wrong_order_metadata_does_not_apply_e() {
			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1")) // 0
					.Chunk(Rec.Prepare(t++, "ab-1")) // 1
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 4, metadata: MaxCount2)) // 2 apply
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 2, metadata: MaxCount1)) // 3 skip
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 3, metadata: MaxCount1)) // 4 skip
					.Chunk(ScavengePointRec(t++, threshold: 1000)))
				.RunAsync();

			Assert.Equal(0, state.SumChunkWeights(0, 0));
			Assert.Equal(0, state.SumChunkWeights(1, 1));
			Assert.Equal(0, state.SumChunkWeights(2, 2));
			Assert.Equal(2, state.SumChunkWeights(3, 3));
			Assert.Equal(2, state.SumChunkWeights(4, 4));

			Assert.True(state.TryGetOriginalStreamData("ab-1", out var originalStreamData));
			Assert.True(state.TryGetMetastreamData("$$ab-1", out var metastreamData));

			Assert.Equal(DiscardPoint.KeepAll, originalStreamData.DiscardPoint);
			Assert.Equal(DiscardPoint.DiscardBefore(4), metastreamData.DiscardPoint);
		}

		[Fact]
		public async Task wrong_order_metadata_then_right_does_apply() {
			var t = 0;
			await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "ab-1"))
					.Chunk(Rec.Prepare(t++, "ab-1"))
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 5, metadata: MaxCount1))
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 0, metadata: MaxCount3))
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", eventNumber: 6, metadata: MaxCount2))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
						x.Recs[0],
						x.Recs[1],
						x.Recs[2].KeepNone(),
						x.Recs[3].KeepNone(),
						x.Recs[4],
						x.Recs[5],
					});
			;
		}
	}
}
