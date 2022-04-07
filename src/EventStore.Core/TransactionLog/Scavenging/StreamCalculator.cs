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
			OriginalStreamData originalStreamData) {

			_lastEventNumber = null;
			_truncateBeforeOrMaxCountDiscardPoint = null;

			OriginalStreamHandle = originalStreamHandle;
			OriginalStreamData = originalStreamData;
		}

		// State that doesn't change. scoped to the scavenge.
		public IIndexReaderForCalculator<TStreamId> Index { get; }
		public ScavengePoint ScavengePoint { get; }

		// State that is scoped to the stream
		public StreamHandle<TStreamId> OriginalStreamHandle { get; private set; }
		public OriginalStreamData OriginalStreamData { get; private set; }

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

		public DiscardPoint TruncateBeforeDiscardPoint =>
			OriginalStreamData.TruncateBefore.HasValue
				? DiscardPoint.DiscardBefore(OriginalStreamData.TruncateBefore.Value)
				: DiscardPoint.KeepAll;

		public DiscardPoint MaxCountDiscardPoint =>
			OriginalStreamData.MaxCount.HasValue
				//qq be careful not to call this with long.max
				? DiscardPoint.DiscardIncluding(LastEventNumber - OriginalStreamData.MaxCount.Value)
				: DiscardPoint.KeepAll;

		// accounts for tombstone, truncateBefore, maxCount.
		private DiscardPoint? _truncateBeforeOrMaxCountDiscardPoint;
		public DiscardPoint TruncateBeforeOrMaxCountDiscardPoint {
			get {
				if (!_truncateBeforeOrMaxCountDiscardPoint.HasValue) {
					_truncateBeforeOrMaxCountDiscardPoint =
						TruncateBeforeDiscardPoint.Or(MaxCountDiscardPoint);
				}

				return _truncateBeforeOrMaxCountDiscardPoint.Value;
			}
		}

		public bool IsTombstoned => OriginalStreamData.IsTombstoned;

		// We can discard the event when it is as old or older than the cutoff
		public DateTime? CutoffTime => ScavengePoint.EffectiveNow - OriginalStreamData.MaxAge;
	}
}
