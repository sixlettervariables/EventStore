using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IIndexReaderForCalculator<TStreamId> {
		//qq maxposition  / positionlimit instead of scavengepoint?
		long GetLastEventNumber(StreamHandle<TStreamId> streamHandle, ScavengePoint scavengePoint);

		//qq needs to return index entries even for deleted streams, and regardless of the metadata
		// applied to the stream
		EventInfo[] ReadEventInfoForward(
			StreamHandle<TStreamId> stream,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);
	}
}
