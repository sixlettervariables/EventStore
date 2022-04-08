using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IOriginalStreamScavengeMap<TStreamId> :
		IScavengeMap<TStreamId, OriginalStreamData> {

		void SetTombstone(TStreamId streamId);
		void SetMetadata(TStreamId streamId, StreamMetadata metadata);
	}
}
