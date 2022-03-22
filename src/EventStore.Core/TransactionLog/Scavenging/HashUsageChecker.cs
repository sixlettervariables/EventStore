using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class HashUsageChecker<TStreamId> : IHashUsageChecker<TStreamId> {
		public bool HashInUseBefore(
			TStreamId streamId,
			ulong hash,
			long position,
			out TStreamId hashUser) {

			//qq look in the index for any record with the current hash up to the limit
			// if any exists then grab the stream name for it
			throw new NotImplementedException();
		}
	}
}
