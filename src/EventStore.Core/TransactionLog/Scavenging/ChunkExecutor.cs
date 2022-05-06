using System;
using System.Collections.Generic;
using System.Threading;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq add logging to this and the other stages
	public class ChunkExecutor<TStreamId, TChunk> : IChunkExecutor<TStreamId> {

		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForChunkExecutor<TStreamId, TChunk> _chunkManager;
		private readonly long _chunkSize;
		private readonly int _cancellationCheckPeriod;

		public ChunkExecutor(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForChunkExecutor<TStreamId, TChunk> chunkManager,
			long chunkSize,
			int cancellationCheckPeriod) {

			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_chunkSize = chunkSize;
			_cancellationCheckPeriod = cancellationCheckPeriod;
		}

		public void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken) {

			var checkpoint = new ScavengeCheckpoint.ExecutingChunks(
				scavengePoint: scavengePoint,
				doneLogicalChunkNumber: default);
			state.SetCheckpoint(checkpoint);
			Execute(checkpoint, state, cancellationToken);
		}

		public void Execute(
			ScavengeCheckpoint.ExecutingChunks checkpoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken) {

			//qq would we want to run in parallel? (be careful with scavenge state interactions
			// in that case, especially writes and storing checkpoints)
			//qq order by the weight? maybe just iterate backwards.

			//qq there is no point scavenging beyond the scavenge point
			//qqqq is +1 ok range wise? same for accumulator
			var startFromChunk = checkpoint?.DoneLogicalChunkNumber + 1 ?? 0; //qq necessarily zero?
			var scavengePoint = checkpoint.ScavengePoint;

			foreach (var physicalChunk in GetAllPhysicalChunks(startFromChunk, scavengePoint.UpToPosition)) {
				var transaction = state.BeginTransaction();
				try {
					var physicalWeight = state.SumChunkWeights(physicalChunk.ChunkStartNumber, physicalChunk.ChunkEndNumber);

					if (physicalWeight >= scavengePoint.Threshold) {
						ExecutePhysicalChunk(scavengePoint, state, physicalChunk, cancellationToken);

						state.ResetChunkWeights(
							physicalChunk.ChunkStartNumber,
							physicalChunk.ChunkEndNumber);

					}

					cancellationToken.ThrowIfCancellationRequested();

					transaction.Commit(
						new ScavengeCheckpoint.ExecutingChunks(
							scavengePoint,
							physicalChunk.ChunkEndNumber));
				} catch {
					transaction.Rollback();
					throw;
				}
			}
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
			IChunkReaderForExecutor<TStreamId> chunk,
			CancellationToken cancellationToken) {

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

			//qqqq TRANSACTIONS
			//qq add tests that makes sure its ok when we have uncommitted transactions that "collide"
			//
			// ChunkExecutor:
			// - we can only scavenge a record if we know what event number it is, for which we need the commit
			//   record. so perhaps we only scavenge the events in a transaction (and the commit record)
			//   if the transaction was committed in the same chunk that it was started. this is pretty much
			//   what the old scavenge does too.
			//
			//  TRANSACTIONS IN OLD SCAVENGE
			//   - looks like uncommitted prepares are generally kept, maybe unless the stream is hard deleted
			//   - looks like even committed prepares are only removed if the commit is in the same chunk
			// - uncommitted transactions are not present in the index, neither are commit records
			//
			//qq what if the lastevent in a stream is in a transaction?
			//     we need to make sure we keep it, even though it doesn't have a expectedversion
			//     so we can do just like old scavenge. if we cant establish the expectedversion then just keep it
			//     if we can establish the expected version then we can compare it to the discard point.
			//qq can we scavenge stuff better if the stream is known to be hard deleted - yes, we can get rid of
			// everything except the begin records (just like old scavenge)
			//   note in this case the lasteventnumber is the tombstone, which is not in a transaction.


			// 1. open the chunk, probably with the bulk reader
			var newChunk = _chunkManager.CreateChunkWriter(
				chunk.ChunkStartNumber,
				chunk.ChunkEndNumber);

			var cancellationCheckCounter = 0;
			foreach (var record in chunk.ReadRecords()) {
				var discard = ShouldDiscard(
					state,
					scavengePoint,
					record);

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

				if (++cancellationCheckCounter == _cancellationCheckPeriod) {
					cancellationCheckCounter = 0;
					cancellationToken.ThrowIfCancellationRequested();
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

			if (!record.IsScavengable)
				return false;

			if (record.EventNumber < 0) {
				//qq we can discard from transactions sometimes, copy the logic from old scavenge.
				return false;
			}

			//qq consider how/where to cache the this stuff per stream for quick lookups
			var details = GetStreamExecutionDetails(
				state,
				record.StreamId);

			// if definitePoint says discard then discard.
			if (details.DiscardPoint.ShouldDiscard(record.EventNumber)) {
				return true;
			}

			// if maybeDiscardPoint says discard then maybe we can discard - depends on maxage
			if (!details.MaybeDiscardPoint.ShouldDiscard(record.EventNumber)) {
				// both discard points said do not discard, so dont.
				return false;
			}

			if (!details.MaxAge.HasValue) {
				return false;
			}

			return record.TimeStamp < scavengePoint.EffectiveNow - details.MaxAge;
		}

		private StreamExecutionDetails GetStreamExecutionDetails(
			IScavengeStateForChunkExecutor<TStreamId> state,
			TStreamId streamId) {

			if (_metastreamLookup.IsMetaStream(streamId)) {
				if (!state.TryGetMetastreamDiscardPoint(streamId, out var discardPoint)) {
					discardPoint = DiscardPoint.KeepAll;
				}

				return new StreamExecutionDetails(
					discardPoint: discardPoint,
					maybeDiscardPoint: DiscardPoint.KeepAll,
					maxAge: null);
			} else {
				// original stream
				if (state.TryGetStreamExecutionDetails(streamId, out var details)) {
					return details;
				} else {
					return new StreamExecutionDetails(
						discardPoint: DiscardPoint.KeepAll,
						maybeDiscardPoint: DiscardPoint.KeepAll,
						maxAge: null);
				}
			}
		}
	}
}
