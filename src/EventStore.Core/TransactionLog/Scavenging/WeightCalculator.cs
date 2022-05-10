using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class WeightCalculator<TStreamId> {
		const float DiscardWeight = 2.0f;
		const float MaybeDiscardWeight = 1.0f;

		private readonly IScavengeStateForCalculator<TStreamId> _state;
		private readonly Dictionary<int, float> _weights;

		public WeightCalculator(IScavengeStateForCalculator<TStreamId> state) {
			_state = state;
			_weights = new Dictionary<int, float>();
		}

		public void OnDiscard(int logicalChunkNumber) => IncreaseChunkWeight(
			logicalChunkNumber,
			DiscardWeight);

		public void OnMaybeDiscard(int logicalChunkNumber) => IncreaseChunkWeight(
			logicalChunkNumber,
			MaybeDiscardWeight);

		private void IncreaseChunkWeight(int logicalChunkNumber, float extraWeight) {
			if (_weights.TryGetValue(logicalChunkNumber, out var current)) {
				_weights[logicalChunkNumber] = extraWeight + current;
			} else {
				_weights[logicalChunkNumber] = extraWeight;
			}
		}

		public void Flush() {
			foreach (var kvp in _weights) {
				var logicalChunkNumber = kvp.Key;
				var extraWeight = kvp.Value;
				_state.IncreaseChunkWeight(logicalChunkNumber, extraWeight);
			}
			_weights.Clear();
		}
	}
}
