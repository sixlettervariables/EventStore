using EventStore.Core.Helpers;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public struct RecordForAccumulatorInitParams<TStreamId> : IReusableObjectInitParams {
		public readonly BasicPrepareLogRecord BasicPrepare;
		public readonly TStreamId StreamId;
		public RecordForAccumulatorInitParams(BasicPrepareLogRecord basicPrepare, TStreamId streamId) {
			BasicPrepare = basicPrepare;
			StreamId = streamId;
		}
	}
}
