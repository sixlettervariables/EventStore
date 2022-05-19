using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IIndexReaderForCalculator<TStreamId> {
		//qq maxposition  / positionlimit instead of scavengepoint?
		long GetLastEventNumber(StreamHandle<TStreamId> streamHandle, ScavengePoint scavengePoint);

		EventInfo[] ReadEventInfoForward(
			StreamHandle<TStreamId> stream,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);
	}
}
