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
		public long GetLastEventNumber(StreamHandle<string> handle, ScavengePoint scavengePoint) {
			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					throw new NotImplementedException();
					//qq index only!
					// return _readIndex.GetStreamLastEventNumber(stream.StreamHash);
				case StreamHandle.Kind.Id:
					// uses log to check for hash collisions
					return _readIndex.GetStreamLastEventNumber(handle.StreamId);
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}

		public EventInfo[] ReadEventInfoForward(
			StreamHandle<string> handle,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) { //qq account for scavengepoint

			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					//qq index only!
					throw new NotImplementedException();
				case StreamHandle.Kind.Id:
					// uses log to check for hash collisions
					var result = _readIndex.ReadStreamEventsForward(
						handle.StreamId,
						fromEventNumber,
						maxCount);

					//qq do we need to look at the other things like .Result, .IsEndOfStream etc
					return null; //qq	result.Records;
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}
	}
}
