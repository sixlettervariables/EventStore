using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IIndexReaderForAccumulator<TStreamId> {
		//qq maxposition  / positionlimit instead of scavengepoint?
		EventInfo[] ReadEventInfoForward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);

		EventInfo[] ReadEventInfoBackward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);
	}
}
