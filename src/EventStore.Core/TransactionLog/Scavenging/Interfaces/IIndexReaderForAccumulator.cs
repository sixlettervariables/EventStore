using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IIndexReaderForAccumulator<TStreamId> {
		EventInfo[] ReadEventInfoForward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount);

		EventInfo[] ReadEventInfoBackward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount);
	}
}
