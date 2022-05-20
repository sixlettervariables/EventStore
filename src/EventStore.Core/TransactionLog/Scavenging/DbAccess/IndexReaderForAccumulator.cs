using System;
using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class IndexReaderForAccumulator<TStreamId> : IIndexReaderForAccumulator<TStreamId> {
		public EventInfo[] ReadEventInfoBackward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			throw new NotImplementedException();
		}

		public EventInfo[] ReadEventInfoForward(
			TStreamId streamId,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			throw new NotImplementedException();
		}
	}
}
