namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryChunkWeightScavengeMap :
		InMemoryScavengeMap<int, float>,
		IChunkWeightScavengeMap {

		public void IncreaseWeight(int logicalChunkNumber, float extraWeight) {
			if (!TryGetValue(logicalChunkNumber, out var weight))
				weight = 0;
			this[logicalChunkNumber] = weight + extraWeight;
		}
	}
}
