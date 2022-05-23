using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	// when scavenging we dont need all the data for a record
	//qq but we do need more data than this
	// but the bytes can just be bytes, in the end we are going to keep it or discard it.
	// recycle this record like the recordforaccumulation?
	// hopefully doesn't have to be a class, or can be pooled
	public abstract class RecordForExecutor<TStreamId, TRecord> {
		public TRecord Record { get; private set; }

		public class Prepare : RecordForExecutor<TStreamId, TRecord> {
			public void SetRecord(
				int length,
				long logPosition,
				TRecord record,
				DateTime timeStamp,
				TStreamId streamId,
				long eventNumber) {

				Length = length;
				LogPosition = logPosition;
				Record = record;
				TimeStamp = timeStamp;
				StreamId = streamId;
				EventNumber = eventNumber;
			}

			//qq check we need all of these
			public int Length { get; private set; }
			public long LogPosition { get; private set; }
			public DateTime TimeStamp { get; private set; }
			public TStreamId StreamId { get; private set; }
			public long EventNumber { get; private set; }
		}

		public class NonPrepare : RecordForExecutor<TStreamId, TRecord> {
			public void SetRecord(TRecord record) {
				Record = record;
			}
		}
	}
}
