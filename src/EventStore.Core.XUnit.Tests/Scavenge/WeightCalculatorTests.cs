using EventStore.Core.LogV2;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class WeightCalculatorTests {
		private readonly WeightCalculator<string> _sut;
		private readonly ScavengeState<string> _state;

		public WeightCalculatorTests() {
			var hasher = new HumanReadableHasher();
			var metastreamLookup = new LogV2SystemStreams();
			_state = new ScavengeStateBuilder(hasher, metastreamLookup).Build();
			_sut = new WeightCalculator<string>(_state);
		}

		[Fact]
		public void sanity() {
			_sut.OnDiscard(0);
			_sut.OnDiscard(1);
			_sut.OnMaybeDiscard(0);

			Assert.Equal(0, _state.SumChunkWeights(0, 0));
			Assert.Equal(0, _state.SumChunkWeights(1, 1));

			_sut.Flush();
			Assert.Equal(3, _state.SumChunkWeights(0, 0));
			Assert.Equal(2, _state.SumChunkWeights(1, 1));

			_sut.OnMaybeDiscard(1);
			_sut.Flush();
			Assert.Equal(3, _state.SumChunkWeights(0, 0));
			Assert.Equal(3, _state.SumChunkWeights(1, 1));
		}
	}
}
