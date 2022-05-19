using System;
using System.Collections.Generic;
using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IScavengeState<TStreamId> :
		IScavengeStateForAccumulator<TStreamId>,
		IScavengeStateForCalculator<TStreamId>,
		IScavengeStateForIndexExecutor<TStreamId>,
		IScavengeStateForChunkMerger,
		IScavengeStateForChunkExecutor<TStreamId>,
		IScavengeStateForCleaner {

		bool TryGetCheckpoint(out ScavengeCheckpoint checkpoint);
	}

	// all the components use these
	public interface IScavengeStateCommon {
		// begin a transaction, returns the started transaction so that it can be
		// committed or rolled back
		ITransactionCompleter BeginTransaction();
	}

	// abstraction to allow the components to commit/rollback
	public interface ITransactionCompleter {
		void Rollback();
		void Commit(ScavengeCheckpoint checkpoint);
	}

	public interface ITransactionManager : ITransactionCompleter{
		void Begin();
	}

	// abstraction for the backing store. memory, sqlite etc.
	public interface ITransactionFactory<TTransaction> {
		TTransaction Begin();
		void Rollback(TTransaction transaction);
		void Commit(TTransaction transaction);
	}

	//qq this summary comment will explain the general shape of the scavenge state - what it needs to be
	// able to store and retrieve and why.
	//
	// There are two kinds of streams that we might want to remove events from
	//    - original streams
	//        - according to tombstone
	//        - according to metadata (maxage, maxcount, tb)
	//    - metadata streams
	//        - according to tombstone
	//        - maxcount 1
	//
	// Together these are the scavengable streams. We store a DiscardPoint for each so that we can
	// 
	// We only need to store metadata for the user streams with metadata since the metadata for
	// metadatastreams is implicit.
	// 
	// however, we need to know about _all_ the stream collisions in the database not just the ones
	// that we might remove events from, so that later we can scavenge the index without looking anything
	// up in the log.

	// accumulator iterates through the log, spotting metadata records
	// put in the data that the chunk and ptable scavenging require
	public interface IScavengeStateForAccumulator<TStreamId> :
		IScavengeStateCommon,
		IIncreaseChunkWeights {

		// call this for each record as we accumulate through the log so that we can spot every hash
		// collision to save ourselves work later.
		// this affects multiple parts of the scavengestate and must be called within a transaction so
		// that its effect is atomic
		void DetectCollisions(TStreamId streamId);

		void SetMetastreamDiscardPoint(TStreamId metastreamId, DiscardPoint discardPoint);

		// for when the _original_ stream is tombstoned, the metadata stream can be removed entirely.
		void SetMetastreamTombstone(TStreamId metastreamId);

		void SetOriginalStreamMetadata(TStreamId originalStreamId, StreamMetadata metadata);

		void SetOriginalStreamTombstone(TStreamId originalStreamId);

		void SetChunkTimeStampRange(int logicalChunkNumber, ChunkTimeStampRange range);
	}

	public interface IScavengeStateForCalculatorReadOnly<TStreamId> : IScavengeStateCommon {
		// Calculator iterates through the scavengable original streams and their metadata
		// it doesn't need to do anything with the metadata streams, accumulator has done those.
		//qq name
		IEnumerable<(StreamHandle<TStreamId>, OriginalStreamData)> OriginalStreamsToScavenge(
			StreamHandle<TStreamId> checkpoint);

		bool TryGetChunkTimeStampRange(int logicaChunkNumber, out ChunkTimeStampRange range);
	}

	public interface IScavengeStateForCalculator<TStreamId> :
		IScavengeStateForCalculatorReadOnly<TStreamId>,
		IIncreaseChunkWeights {

		void SetOriginalStreamDiscardPoints(
			StreamHandle<TStreamId> streamHandle,
			CalculationStatus status,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint);
	}

	public interface IIncreaseChunkWeights {
		void IncreaseChunkWeight(int logicalChunkNumber, float extraWeight);
	}

	public interface IScavengeStateForChunkExecutor<TStreamId> : IScavengeStateCommon {
		float SumChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber);
		void ResetChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber);
		bool TryGetChunkExecutionInfo(TStreamId streamId, out ChunkExecutionInfo info);
		bool TryGetMetastreamData(TStreamId streamId, out MetastreamData metastreamData);
	}

	public interface IScavengeStateForChunkMerger : IScavengeStateCommon {
	}

	public interface IScavengeStateForIndexExecutor<TStreamId> : IScavengeStateCommon {
		bool IsCollision(ulong streamHash);
		//qq precondition: the streamhandle must be of the correct kind.
		bool TryGetIndexExecutionInfo(
			StreamHandle<TStreamId> streamHandle,
			out IndexExecutionInfo info);
	}

	public interface IScavengeStateForCleaner : IScavengeStateCommon {
		bool AllChunksExecuted();
		void DeleteMetastreamData();
		void DeleteOriginalStreamData(bool deleteArchived);
	}
}
