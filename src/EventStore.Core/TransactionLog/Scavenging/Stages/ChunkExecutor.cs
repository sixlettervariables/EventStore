using System;
using System.Collections.Generic;
using System.Threading;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkExecutor<TStreamId, TRecord> : IChunkExecutor<TStreamId> {

		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForChunkExecutor<TStreamId, TRecord> _chunkManager;
		private readonly long _chunkSize;
		private readonly bool _unsafeIgnoreHardDeletes;
		private readonly int _cancellationCheckPeriod;

		public ChunkExecutor(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForChunkExecutor<TStreamId, TRecord> chunkManager,
			long chunkSize,
			bool unsafeIgnoreHardDeletes,
			int cancellationCheckPeriod) {

			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_chunkSize = chunkSize;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
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

			var startFromChunk = checkpoint?.DoneLogicalChunkNumber + 1 ?? 0;
			var scavengePoint = checkpoint.ScavengePoint;

			foreach (var physicalChunk in GetAllPhysicalChunks(startFromChunk, scavengePoint)) {
				var transaction = state.BeginTransaction();
				try {
					var physicalWeight = state.SumChunkWeights(
						physicalChunk.ChunkStartNumber,
						physicalChunk.ChunkEndNumber);

					if (physicalWeight > scavengePoint.Threshold || _unsafeIgnoreHardDeletes) {
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
					//qq here might be sensible place, the old scavenge handles various exceptions
					// FileBeingDeletedException, OperationCanceledException, Exception
					// with logging and without stopping the scavenge i think. consider what we want to do
					// but be careful that if we allow the scavenge to continue without having executed
					// this chunk, we can't assume later that the scavenge was really completed, which
					// has implications for the cleaning phase, especially with _unsafeIgnoreHardDeletes
					transaction.Rollback();
					throw;
				}
			}
		}

		private IEnumerable<IChunkReaderForExecutor<TStreamId, TRecord>> GetAllPhysicalChunks(
			int startFromChunk,
			ScavengePoint scavengePoint) {

			var scavengePos = _chunkSize * startFromChunk;
			var upTo = scavengePoint.Position;
			while (scavengePos < upTo) {
				// in bounds because we stop before the scavenge point
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
			IChunkReaderForExecutor<TStreamId, TRecord> sourceChunk,
			CancellationToken cancellationToken) {

			var outputChunk = _chunkManager.CreateChunkWriter(sourceChunk);

			var cancellationCheckCounter = 0;
			var discardedCount = 0;
			var keptCount = 0;

			// nonPrepareRecord and prepareRecord ae reused through the iteration
			var nonPrepareRecord = new RecordForExecutor<TStreamId, TRecord>.NonPrepare();
			var prepareRecord = new RecordForExecutor<TStreamId, TRecord>.Prepare();

			foreach (var isPrepare in sourceChunk.ReadInto(nonPrepareRecord, prepareRecord)) {
				if (isPrepare) {
					if (ShouldDiscard(state, scavengePoint, prepareRecord)) {
						discardedCount++;
					} else {
						keptCount++;
						outputChunk.WriteRecord(prepareRecord);
					}
				} else {
					keptCount++;
					outputChunk.WriteRecord(nonPrepareRecord);
				}

				if (++cancellationCheckCounter == _cancellationCheckPeriod) {
					cancellationCheckCounter = 0;
					cancellationToken.ThrowIfCancellationRequested();
				}
			}

			outputChunk.SwitchIn(out var newFileName);

			//qq what is the new file name of an inmemory chunk :/
			//qq log
		}

		private bool ShouldDiscard(
			IScavengeStateForChunkExecutor<TStreamId> state,
			ScavengePoint scavengePoint,
			RecordForExecutor<TStreamId, TRecord>.Prepare record) {

			// the discard points ought to be sufficient, but sometimes this will be quicker
			// and it is a nice safty net
			if (record.LogPosition >= scavengePoint.Position)
				return false;

			if (record.EventNumber < 0) {
				// we could discard from transactions sometimes, either by accumulating a state for them
				// or doing a similar trick as old scavenge and limiting it to transactions that were
				// stated and commited in the same chunk. however for now this isn't considered so
				// important because someone with transactions to scavenge has probably scavenged them
				// already with old scavenge. could be added later
				return false;
			}

			//qq consider how/where to cache the this stuff per stream for quick lookups
			var details = GetStreamExecutionDetails(
				state,
				record.StreamId);

			if (details.IsTombstoned) {
				if (_unsafeIgnoreHardDeletes) {
					// remove _everything_ for metadata and original streams
					return true;
				}

				if (_metastreamLookup.IsMetaStream(record.StreamId)) {
					// when the original stream is tombstoned we can discard the _whole_ metadata stream
					return true;
				}

				// otherwise obey the discard points below.
			}

			// if definitePoint says discard then discard.
			if (details.DiscardPoint.ShouldDiscard(record.EventNumber)) {
				return true;
			}

			// if maybeDiscardPoint says discard then maybe we can discard - depends on maxage
			if (!details.MaybeDiscardPoint.ShouldDiscard(record.EventNumber)) {
				// both discard points said do not discard, so dont.
				return false;
			}

			// discard said no, but maybe discard said yes
			if (!details.MaxAge.HasValue) {
				return false;
			}

			return record.TimeStamp < scavengePoint.EffectiveNow - details.MaxAge;
		}

		private ChunkExecutionInfo GetStreamExecutionDetails(
			IScavengeStateForChunkExecutor<TStreamId> state,
			TStreamId streamId) {

			if (_metastreamLookup.IsMetaStream(streamId)) {
				if (!state.TryGetMetastreamData(streamId, out var metastreamData)) {
					metastreamData = MetastreamData.Empty;
				}

				return new ChunkExecutionInfo(
					isTombstoned: metastreamData.IsTombstoned,
					discardPoint: metastreamData.DiscardPoint,
					maybeDiscardPoint: DiscardPoint.KeepAll,
					maxAge: null);
			} else {
				// original stream
				if (state.TryGetChunkExecutionInfo(streamId, out var details)) {
					return details;
				} else {
					return new ChunkExecutionInfo(
						isTombstoned: false,
						discardPoint: DiscardPoint.KeepAll,
						maybeDiscardPoint: DiscardPoint.KeepAll,
						maxAge: null);
				}
			}
		}
	}
}
