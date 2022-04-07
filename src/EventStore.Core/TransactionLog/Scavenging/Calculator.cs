using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Calculator<TStreamId> : ICalculator<TStreamId> {
		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IIndexReaderForCalculator<TStreamId> _index;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly int _chunkSize;

		public Calculator(
			ILongHasher<TStreamId> hasher,
			IIndexReaderForCalculator<TStreamId> index,
			IMetastreamLookup<TStreamId> metastreamLookup,
			int chunkSize) {

			_hasher = hasher;
			_index = index;
			_metastreamLookup = metastreamLookup;
			_chunkSize = chunkSize;
		}

		public void Calculate(
			ScavengePoint scavengePoint,
			IScavengeStateForCalculator<TStreamId> state) {

			var streamCalc = new StreamCalculator<TStreamId>(_index, scavengePoint);
			var eventCalc = new EventCalculator<TStreamId>(_chunkSize, state, scavengePoint, streamCalc);

			// iterate through the original (i.e. non-meta) streams that need scavenging (i.e.
			// those that have metadata or tombstones)
			// - for each one use the accumulated data to set/update the discard points of the stream.
			// - along the way add weight to the affected chunks.
			foreach (var (originalStreamHandle, originalStreamData) in state.OriginalStreamsToScavenge) {
				//qq it would be neat if this interface gave us some hint about the location of
				// the DP so that we could set it in a moment cheaply without having to search.
				// although, if its a wal that'll be cheap anyway.
				//qq if the scavengemap supports RMW that might have a bearing too, but for now maybe
				// this is just overcomplicating things.

				streamCalc.SetStream(originalStreamHandle, originalStreamData);

				CalculateDiscardPointForOriginalStream(
					eventCalc,
					state,
					originalStreamHandle,
					scavengePoint,
					out var adjustedDiscardPoint,
					out var adjustedMaybeDiscardPoint);

				if (adjustedDiscardPoint == originalStreamData.DiscardPoint &&
					adjustedMaybeDiscardPoint == originalStreamData.MaybeDiscardPoint) {

					// nothing to update for this stream
					continue;
				}

				var newOriginalStreamData = new OriginalStreamData {
					IsTombstoned = originalStreamData.IsTombstoned,
					MaxAge = originalStreamData.MaxAge,
					MaxCount = originalStreamData.MaxCount,
					TruncateBefore = originalStreamData.TruncateBefore,

					DiscardPoint = adjustedDiscardPoint,
					MaybeDiscardPoint = adjustedMaybeDiscardPoint,
				};

				state.SetOriginalStreamData(originalStreamHandle, newOriginalStreamData);
			}
		}

		// This does two things.
		// 1. Calculates and returns the discard point
		// 2. Adds weight to the affected chunks so that they get scavenged.
		private void CalculateDiscardPointForOriginalStream(
			EventCalculator<TStreamId> eventCalc,
			IScavengeStateForCalculator<TStreamId> state,
			StreamHandle<TStreamId> originalStreamHandle,
			ScavengePoint scavengePoint,
			out DiscardPoint discardPoint,
			out DiscardPoint maybeDiscardPoint) {

			// iterate through the eventInfos in slices.
			// add weight to the chunk the event is in if are discarding or maybe discarding it
			// move the finalDiscardPoint if we are definitely discarding the event.

			//qq this gets set again if we discard something. so, if we don't discard something, what do
			// we want to happen? we probably want to keep it as what we calculated last time - but this
			// will only become apparent when we do subsequent scavenges.
			discardPoint = default;
			//qq this gets set again if we maybe-discard something. same question as above.
			maybeDiscardPoint = default;

			var fromEventNumber = 0L; //qq maybe from the previous scavenge point
			while (true) {
				// read in slices because the stream might be huge.
				// Note: when the handle is a hash the ReadEventInfoForward call is index-only
				//qq limit the read to the scavengepoint too?
				const int maxCount = 100; //qq what would be sensible? probably pretty large

				var slice = _index.ReadEventInfoForward(
					originalStreamHandle,
					fromEventNumber,
					maxCount,
					scavengePoint);

				const float DiscardWeight = 2.0f;
				const float MaybeDiscardWeight = 1.0f;

				//qq naive, we dont need to check every event, we could check the last one**
				foreach (var eventInfo in slice) {
					eventCalc.SetEvent(eventInfo);

					switch (eventCalc.DecideEvent()) {
						case DiscardDecision.Discard:
							AddWeightToChunk(state, eventCalc.LogicalChunkNumber, DiscardWeight);
							discardPoint = DiscardPoint.DiscardIncluding(eventInfo.EventNumber);
							break;

						case DiscardDecision.MaybeDiscard:
							// add weight to the chunk so that this will be inspected more closely
							AddWeightToChunk(state, eventCalc.LogicalChunkNumber, MaybeDiscardWeight);
							maybeDiscardPoint = DiscardPoint.DiscardIncluding(eventInfo.EventNumber);
							break;

						case DiscardDecision.Keep:
							// found the first one to keep. we are done discarding.
							return;

						default:
							throw new Exception("sdfhg"); //qq detail
					}
				}

				if (slice.Length < maxCount) {
					//qq we discarded everything in the stream, this should never happen
					// since we always keep the last event (..unless ignore hard deletes
					// is enabled)
					// which ones are otherwise in danger of removing all the events?
					//  hard deleted?
					//
					//qq although, the old scavenge might be capable of removing all the events
					// after this scavenge point... which would produce this condition.
					//
					// in these situatiosn what discard point should we return, or do we need to abort
					throw new Exception("panic"); //qq dont panic really shouldn't
				}

				fromEventNumber += slice.Length;
			}
		}

		// figure out which chunk it is for and note it down
		//qq chunk instructions are per logical chunk (for now)
		private void AddWeightToChunk(
			IScavengeStateForCalculator<TStreamId> state,
			int logicalChunkNumber,
			float extraWeight) {

			//qq dont go lookin it up every time, hold on to one set of chunkinstructions until we
			// have made it to the next chunk.
			if (!state.TryGetChunkWeight(logicalChunkNumber, out var weight))
				weight = 0;
			state.SetChunkWeight(logicalChunkNumber, weight + extraWeight);
		}
	}
}
