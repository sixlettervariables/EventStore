using System;
using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class MaxAgeTests : ScavengerTestsBase {
		protected static StreamMetadata MaxAgeMetadata { get; } =
			new StreamMetadata(maxAge: TimeSpan.FromDays(2));

		protected static DateTime Expired { get; } = EffectiveNow - TimeSpan.FromDays(3);
		protected static DateTime Active { get; } = EffectiveNow - TimeSpan.FromDays(1);

		[Fact]
		public void simple_maxage() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1", timestamp: Expired),
					Rec.Prepare(1, "ab-1", timestamp: Expired),
					Rec.Prepare(2, "ab-1", timestamp: Active),
					Rec.Prepare(3, "ab-1", timestamp: Active),
					Rec.Prepare(4, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
				.CompleteLastChunk())
				.Run(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3, 4)
					},
					x => new[] {
						x.Recs[0]
					});
		}

		[Fact]
		public void keep_last_event() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1", timestamp: Expired),
					Rec.Prepare(1, "ab-1", timestamp: Expired),
					Rec.Prepare(2, "ab-1", timestamp: Expired),
					Rec.Prepare(3, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
				.CompleteLastChunk())
				.Run(
					x => new[] {
						x.Recs[0].KeepIndexes(2, 3)
					},
					x => new[] {
						x.Recs[0]
					});
		}

		//qq test when metadata is set first
		//qq test when maxage can remove things from the index
	}

}
