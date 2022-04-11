using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IOriginalStreamScavengeMap<TKey> :
		IScavengeMap<TKey, OriginalStreamData> {

		void SetTombstone(TKey key);

		void SetMetadata(TKey key, StreamMetadata metadata);

		void SetDiscardPoints(
			TKey key,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint);

		bool TryGetStreamExecutionDetails(TKey key, out StreamExecutionDetails details);
	}
}
