namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IChunkWeightScavengeMap : IScavengeMap<int, float> {
		void IncreaseWeight(int logicalChunkNumber, float extraWeight);
	}
}
