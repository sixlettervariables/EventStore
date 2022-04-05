using System;
using System.Collections.Generic;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkExecutor<TStreamId, TChunk> : IChunkExecutor<TStreamId> {

		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForChunkExecutor<TStreamId, TChunk> _chunkManager;
		private readonly long _chunkSize;

		public ChunkExecutor(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForChunkExecutor<TStreamId, TChunk> chunkManager,
			long chunkSize) {

			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_chunkSize = chunkSize;
		}

		public void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> scavengeState) {

			//qq would we want to run in parallel? (be careful with scavenge state interactions
			// in that case, especially writes)
			//qq order by the weight? maybe just iterate backwards.

			//qq there is no point scavenging beyond the scavenge point
			// but we coul
			var startFromChunk = 0; //qq necessarily zero?

			foreach (var physicalChunk in GetAllPhysicalChunks(startFromChunk, scavengePoint.Position)) {
				var physicalWeight = WeighPhysicalChunk(scavengeState, physicalChunk);

				//qq configurable threshold? in scavenge point?
				var threshold = 0.0f;
				if (physicalWeight < threshold) {
					// they'll still (typically) be removed from the index
					return;
				}

				ExecutePhysicalChunk(scavengePoint, scavengeState, physicalChunk);

				foreach (var logicalChunkNumber in physicalChunk.LogicalChunkNumbers) {
					//qq perhaps removing the chunk weight rather than setting it to zero
					scavengeState.SetChunkWeight(logicalChunkNumber, 0);
				}
			}
		}

		private float WeighPhysicalChunk(
			IScavengeStateForChunkExecutor<TStreamId> scavengeState,
			IChunkReaderForExecutor<TStreamId> physicalChunk) {

			// add together the weights of each of the logical chunks in this physical chunk.
			var totalWeight = 0.0f;
			foreach (var logicalChunkNumber in physicalChunk.LogicalChunkNumbers) {
				if (scavengeState.TryGetChunkWeight(logicalChunkNumber, out var weight)) {
					totalWeight += weight;
				}
			}

			return totalWeight;
		}

		private IEnumerable<IChunkReaderForExecutor<TStreamId>> GetAllPhysicalChunks(
			int startFromChunk,
			long upTo) {

			var scavengePos = _chunkSize * startFromChunk;
			while (scavengePos < upTo) {
				var physicalChunk = _chunkManager.GetChunkReaderFor(scavengePos);

				if (!physicalChunk.IsReadOnly)
					yield break;

				yield return physicalChunk;

				scavengePos = physicalChunk.ChunkEndPosition;
			}

		}

		private void ExecutePhysicalChunk(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			IChunkReaderForExecutor<TStreamId> chunk) {

			//qq the other reason we might want to not scanvenge this chunk is if the posmap would make
			// it bigger
			// than the original... limited concern because of the threshold above BUT we could address
			// by using a padding/scavengedevent system event to prevent having to write a posmap
			// this is the kind of decision we can make in here, local to the chunk.
			// knowing the numrecordstodiscard could be useful here, if we are just discarding a small
			// number then we'd probably pad them with 'gone' events instead of adding a posmap.

			//qq in ExecuteChunk could also be a reasonable place to do a best effort at removing commit
			// records if all the prepares for the commit are in this chunk (typically the case) and they
			// are all scavenged, then we can remove the commit as well i think. this is probably what
			// the old scavenge does. check

			//qq old scavenge says 'never delete the very first prepare in a transaction'
			// hopefully we can account for that here? although maybe it means our count of
			// records to scavenge that was calculated index only might end up being approximate.

			// 1. open the chunk, probably with the bulk reader
			var newChunk = _chunkManager.CreateChunkWriter(
				chunk.ChunkStartNumber,
				chunk.ChunkEndNumber);

			foreach (var record in chunk.ReadRecords()) {
				var discard = ShouldDiscard(
					state,
					scavengePoint,
					record);

				//qq hmm events in transactions do not have an EventNumber
				if (discard) {
					//qq discard record
				} else {
					//qq keep record
					newChunk.WriteRecord(record); //qq or similar
					//qq do we need to upgrade it?
					//qq will using the bulk reader be awkward considering the record format
					// size changes that have occurred over the years
					// if so consider using the regular reader.
					// what does the old scavenge use
					// consider transactions
				}
			}
			// 2. read through it, keeping and discarding as necessary. probably no additional lookups at
			// this point
			// 3. write the posmap
			// 4. finalise the chunk
			// 5. swap it in to the chunkmanager
			if (_chunkManager.TrySwitchChunk(
				newChunk.WrittenChunk,
				verifyHash: default, //qq
				removeChunksWithGreaterNumbers: default, //qq
				out var newFileName)) {
				//qq what is the new file name of an inmemory chunk :/
				//qq log
			} else {
				//qq log
			}
		}

		private bool ShouldDiscard(
			IScavengeStateForChunkExecutor<TStreamId> state,
			ScavengePoint scavengePoint,
			RecordForScavenge<TStreamId> record) {

			//qq consider how/where to cache the this stuff per stream for quick lookups
			GetStreamExecutionDetails(
				state,
				record.StreamId,
				out var definitePoint,
				out var maybeDiscardPoint,
				out var maxAge);

			// if definitePoint says discard then discard.
			if (definitePoint.ShouldDiscard(record.EventNumber)) {
				return true;
			}

			// if maybeDiscardPoint says discard then maybe we can discard - depends on maxage
			if (!maybeDiscardPoint.ShouldDiscard(record.EventNumber)) {
				// both discard points said do not discard, so dont.
				return false;
			}

			if (!maxAge.HasValue) {
				return false;
			}

			return record.TimeStamp < scavengePoint.EffectiveNow - maxAge;
		}

		private void GetStreamExecutionDetails(
			IScavengeStateForChunkExecutor<TStreamId> state,
			TStreamId streamId,
			out DiscardPoint discardPoint,
			out DiscardPoint maybeDiscardPoint,
			out TimeSpan? maxAge) {

			if (_metastreamLookup.IsMetaStream(streamId)) {
				
				maxAge = null;
				maybeDiscardPoint = DiscardPoint.KeepAll;

				if (state.TryGetMetastreamData(streamId, out var metaStreamData)) {
					discardPoint = metaStreamData.DiscardPoint;
				} else {
					discardPoint = DiscardPoint.KeepAll;
				}
			} else {
				// original stream
				if (state.TryGetOriginalStreamData(streamId, out var originalStreamData)) {
					discardPoint = originalStreamData.DiscardPoint;
					maybeDiscardPoint = originalStreamData.MaybeDiscardPoint;
					maxAge = originalStreamData.MaxAge;
				} else {
					discardPoint = DiscardPoint.KeepAll;
					maybeDiscardPoint = DiscardPoint.KeepAll;
					maxAge = null;
				}
			}
		}
	}
}
