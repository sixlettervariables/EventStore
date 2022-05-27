using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IIndexReaderForAccumulator<TStreamId> {
		//qq maxposition  / positionlimit instead of scavengepoint?
		EventInfo[] ReadEventInfoForward(
			StreamHandle<TStreamId> handle,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);

		EventInfo[] ReadEventInfoBackward(
			TStreamId streamId,
			StreamHandle<TStreamId> handle,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);
	}
}
