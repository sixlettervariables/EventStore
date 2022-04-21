using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class SubsequentScavengeTests {
		[Fact]
		public async Task Foo() {
			var (state, _) = await new Scenario()
				.WithDb(x => x
					.Chunk(
						Rec.Prepare(0, "cd-cancel-calculation"),
						Rec.Prepare(1, "$$cd-cancel-calculation", metadata: MaxCount1))
					.CompleteLastChunk())
				.CancelWhenCalculatingOriginalStream("cd-cancel-calculation")
				.RunAsync();

			Assert.True(state.TryGetCheckpoint(out var checkpoint));
			var calculating = Assert.IsType<ScavengeCheckpoint.Calculating<string>>(checkpoint);
			Assert.Equal(default, calculating.DoneStreamHandle);
		}
	}
}
