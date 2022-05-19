using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq implement performance overrides as necessary for this struct and others
	// (DiscardPoint, StreamHandle, ..)
	//qq for all of thes consider how much precision (and therefore bits) we need
	//qq look at the places where we construct this, are we always setting what we need
	// might want to make an explicit constructor. can we easily find the places we are calling 'with' ?
	//qq for everything in here consider signed/unsigned and the number of bits and whether it needs to
	// but nullable vs, say, using -1 to mean no value.
	//qq consider whether to make this immutable or reusable and make sure we are using it appropriately.
	// ^ i think the above was about metastreamdata

	// store a range per chunk so that the calculator can definitely get a timestamp range for each event
	// that is guaranteed to to contain the real timestamp of that event.
	public struct ChunkTimeStampRange {
		public ChunkTimeStampRange(DateTime min, DateTime max) {
			Min = min;
			Max = max;
		}

		public DateTime Min { get; }

		public DateTime Max { get; }
	}
}
