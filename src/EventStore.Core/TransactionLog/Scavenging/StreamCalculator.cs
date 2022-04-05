using System;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq name
	// but the idea of this is we load some context into this class for the given stream
	// and then it helps us calculate and make decisions about what to keep.
	// and the calculations can be done on demand.
	// and can be unit tested separately.
	// reused between streams to avoid allocations.
	//qq the observation here is that various properties exist for the stream, which we might
	// or might not need to calculate, and want to be clear that they are not mutating
	// factoring them out here helps to manage this more cleanly, rather than having a rather large
	// method

	public class StreamCalculator<TStreamId> {
		public StreamCalculator(
			IIndexReaderForCalculator<TStreamId> index,
			ScavengePoint scavengePoint) {

			Index = index;
			ScavengePoint = scavengePoint;
		}

		public void SetStream(
			StreamHandle<TStreamId> originalStreamHandle,
			MetastreamData metadata,
			bool isTombstoned,
			DiscardPoint accumulatedDiscardPoint) {

			_lastEventNumber = null;
			OriginalStreamHandle = originalStreamHandle;
			Metadata = metadata;
			IsTombstoned = isTombstoned;
			AccumulatedDiscardPoint = accumulatedDiscardPoint;
		}

		// State that doesn't change. scoped to the scavenge.
		public IIndexReaderForCalculator<TStreamId> Index { get; }
		public ScavengePoint ScavengePoint { get; }

		// State that is scoped to the stream
		public StreamHandle<TStreamId> OriginalStreamHandle { get; private set; }
		public MetastreamData Metadata { get; private set; }
		public bool IsTombstoned { get; private set; }
		public DiscardPoint AccumulatedDiscardPoint { get; private set; }

		//qq consider what will happen here if the strea, doesn't exist
		//  if it doesn't exist at all then presumably there is nothing to scavenge
		//    we can set the disard point to anything
		//  if it doesn't exist before the scavenge point but does later then
		//    there is nothing to remove as part of this scavenge, but we need to be careful
		//    not to remove the later events.
		private long? _lastEventNumber;
		public long LastEventNumber {
			get {
				if (!_lastEventNumber.HasValue) {
					_lastEventNumber = Index.GetLastEventNumber(OriginalStreamHandle, ScavengePoint);
				}

				return _lastEventNumber.Value;
			}
		}

		// for original streams, the discardPoint from the accumulator applies the tombstone only.
		//qq ^ although, consider for subsequent scavenges.
		public DiscardPoint TombstoneDiscardPoint =>
			AccumulatedDiscardPoint;

		public DiscardPoint TruncateBeforeDiscardPoint =>
			Metadata.TruncateBefore.HasValue
				? DiscardPoint.DiscardBefore(Metadata.TruncateBefore.Value)
				: DiscardPoint.KeepAll;

		public DiscardPoint MaxCountDiscardPoint =>
			Metadata.MaxCount.HasValue
				//qq be careful not to call this with long.max
				? DiscardPoint.DiscardIncluding(LastEventNumber - Metadata.MaxCount.Value)
				: DiscardPoint.KeepAll;

		// We can discard the event when it is as old or older than the cutoff
		public DateTime? CutoffTime => ScavengePoint.EffectiveNow - Metadata.MaxAge;
	}
}
