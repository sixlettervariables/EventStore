using System;
using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq might be overkill in the end, but lets create it for now to help write the code
	public class EventCalculator<TStreamId> {
		public EventCalculator(
			int chunkSize,
			IScavengeStateForCalculatorReadOnly<TStreamId> state,
			ScavengePoint scavengePoint,
			StreamCalculator<TStreamId> streamCalc) {

			ChunkSize = chunkSize;
			State = state;
			ScavengePoint = scavengePoint;
			Stream = streamCalc;
		}

		public void SetEvent(EventInfo eventInfo) {
			EventInfo = eventInfo;
		}

		// State that doesn't change. scoped to the scavenge.
		public int ChunkSize { get; }
		public IScavengeStateForCalculatorReadOnly<TStreamId> State { get; }
		public ScavengePoint ScavengePoint { get; }
		public StreamCalculator<TStreamId> Stream { get; }

		// State that is scoped to the event.
		public EventInfo EventInfo { get; private set; }

		//qq consider this inequality
		public bool IsLastEventInStream => EventInfo.EventNumber == Stream.LastEventNumber;

		public bool IsOnOrAfterScavengePoint => EventInfo.LogPosition >= ScavengePoint.UpToPosition;

		//qq consider caching
		public int LogicalChunkNumber => (int)(EventInfo.LogPosition / ChunkSize);

		public DiscardDecision DecideEvent() {
			// Events in original streams can be discarded because of:
			//   Tombstones, TruncateBefore, MaxCount, MaxAge.
			//
			// any one of these is enough to warrant a discard
			// however none of them allow us to scavenge the last event
			// or anything beyond the scavenge point, so we limit by that.

			// respect the scavenge point
			//qq possibly it should be impossible to get here because we should have run into
			// LastEventInStream (before scavengepoint) by now, unless there wasn't one in the range we
			// read..
			if (IsOnOrAfterScavengePoint) {
				return DiscardDecision.Keep;
			}

			// keep last event instream
			if (IsLastEventInStream) {
				return DiscardDecision.Keep;
			}

			// for tombstoned streams, discard everything that isn't the last event
			if (Stream.IsTombstoned) {
				// we already know this is not the last event, so discard it.
				return DiscardDecision.Discard;
			}

			// truncatebefore, maxcount
			if (Stream.TruncateBeforeOrMaxCountDiscardPoint.ShouldDiscard(EventInfo.EventNumber)) {
				return DiscardDecision.Discard;
			}

			// up to maxage. keep if there is no maxage restriction
			if (!Stream.CutoffTime.HasValue) {
				return DiscardDecision.Keep;
			}

			// there is a maxage restriction
			return ShouldDiscardForMaxAge(Stream.CutoffTime.Value);
		}

		private DiscardDecision ShouldDiscardForMaxAge(DateTime cutoffTime) {
			// establish a the range that the event was definitely created between.
			if (!State.TryGetChunkTimeStampRange(LogicalChunkNumber, out var timeStampRange)) {
				throw new Exception("Couldn't get TimeStamp range"); //qq details
			}

			// range is guanranteed to be non-empty
			//qq consider whether either/both of these should include equal-to.
			if (cutoffTime < timeStampRange.Min) {
				return DiscardDecision.Keep;
			}

			if (cutoffTime > timeStampRange.Max) {
				return DiscardDecision.Discard;
			}

			// must be between min and max inclusive
			return DiscardDecision.MaybeDiscard;
		}
	}
}
