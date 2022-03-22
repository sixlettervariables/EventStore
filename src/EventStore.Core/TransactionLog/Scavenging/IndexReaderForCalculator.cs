using System;
using EventStore.Core.Services.Storage.ReaderIndex;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq consider whether to use IReadIndex or TableIndex directly
	public class IndexReaderForCalculator : IIndexReaderForCalculator<string> {
		private readonly IReadIndex _readIndex;

		public IndexReaderForCalculator(IReadIndex readIndex) {
			_readIndex = readIndex;
		}

		//qq todo respect scavengepoint
		public long GetLastEventNumber(StreamHandle<string> stream, ScavengePoint scavengePoint) {
			if (stream.IsHash) {
				throw new NotImplementedException();
				//qq index only!
				// return _readIndex.GetStreamLastEventNumber(stream.StreamHash);
			} else {
				// uses log to check for hash collisions
				return _readIndex.GetStreamLastEventNumber(stream.StreamId);
			}
		}

		public EventInfo[] ReadEventInfoForward(
			StreamHandle<string> stream,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) { //qq account for scavengepoint

			if (stream.IsHash) {
				//qq index only!
				throw new NotImplementedException();
			} else {
				// uses log to check for hash collisions
				var result = _readIndex.ReadStreamEventsForward(
					stream.StreamId,
					fromEventNumber,
					maxCount);

				//qq do we need to look at the other things like .Result, .IsEndOfStream etc
				return null; //qq	result.Records;
			}
		}
	}
}
