using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	// when scavenging we dont need all the data for a record
	//qq but we do need more data than this
	// but the bytes can just be bytes, in the end we are going to keep it or discard it.
	// recycle this record like the recordforaccumulation?
	// hopefully doesn't have to be a class, or can be pooled
	public class RecordForScavenge {
		public static RecordForScavenge<TStreamId> CreateScavengeable<TStreamId>(
				TStreamId streamId,
				DateTime timeStamp,
				long eventNumber,
				byte[] bytes) {

			return new RecordForScavenge<TStreamId>() {
				IsScavengable = true,
				TimeStamp = timeStamp,
				StreamId = streamId,
				EventNumber = eventNumber,
				RecordBytes = bytes,
			};
		}

		public static RecordForScavenge<TStreamId> CreateNonScavengeable<TStreamId>(
			byte[] bytes) {

			return new RecordForScavenge<TStreamId>() {
				IsScavengable = false,
				RecordBytes = bytes,
			};
		}
	}

	public class RecordForScavenge<TStreamId> {
		public bool IsScavengable { get; set; } //qq temporary messy
		public TStreamId StreamId { get; set; }
		public DateTime TimeStamp { get; set; }
		public long EventNumber { get; set; }
		//qq pool
		public byte[] RecordBytes { get; set; }
	}
}
