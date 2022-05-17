using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Helpers;
using EventStore.Core.Index;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	// There are two kinds of streams that we might want to remove events from
	//    - original streams (i.e. streams with metadata)
	//        - according to tombstone
	//        - according to metadata (maxage, maxcount, tb)
	//    - metadata streams
	//        - according to tombstone
	//        - maxcount 1
	//
	// In a nutshell:
	// - The Accumulator passes through the log once (in total, not per scavenge)
	//   accumulating state that we need and calculating some DiscardPoints.
	// - When a ScavengePoint is set, the Calculator uses it to finish calculating
	//   the DiscardPoints.
	// - The Chunk and Index Executors can then use this information to perform the
	//   actual record/indexEntry removal.

	public interface IScavenger {
		//qq probably we want this to continue a previous scavenge if there is one going,
		// or start a new one otherwise.
		Task RunAsync(ITFChunkScavengerLog scavengerLogger, CancellationToken cancellationToken);
		//qq options
		// - timespan, or datetime to autostop
		// - chunk to scavenge up to
		// - effective 'now'
		// - remove open transactions : bool
	}

	// The Accumulator reads through the log up to the scavenge point
	// its purpose is to do any log scanning that is necessary for a scavenge _only once_
	// accumulating whatever state is necessary to avoid subsequent scans.
	//
	// in practice it populates the scavenge state with:
	//  1. the scavengable streams
	//  2. hash collisions between any streams
	//  3. most recent metadata
	//  4. discard points according to TombStones, MetadataMaxCount1, TruncateBefore.
	//qq 5. data for maxage calculations - maybe that can be another IScavengeMap
	public interface IAccumulator<TStreamId> {
		void Accumulate(
			ScavengePoint prevScavengePoint,
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			CancellationToken cancellationToken);

		void Accumulate(
			ScavengeCheckpoint.Accumulating checkpoint,
			IScavengeStateForAccumulator<TStreamId> state,
			CancellationToken cancellationToken);
	}

	// The Calculator calculates the DiscardPoints that depend on the ScavengePoint
	// (after which all the scavengable streams will have discard points calculated correctly)
	//
	// It also creates a heuristic for which chunks are most in need of scavenging.
	//  - an approximate count of the number of records to discard in each chunk
	//     //qq explain why it is approximate. 
	//         - we don't count commit records (but, i think, if we are able to scavenge a commit record
	//            then we will have increased the weight of the chunk for the sake of the event records	//             
	//         - we don't currently count metadata records, but we probably could if its worth it
	//             each time we find a metadata record we need to increment the count for the chunk
	//             of the _old_ metadata record, which we can find because we stored its position yay
	//         - any other reason?
	//
	// The job of calculating the DiscardPoints is split between the Accumulator and the Calculator.
	// Some things affect the DiscardPoints in a fairly static way and can be applied to the
	// DiscardPoint directly in the Accumulator. Others set criteria for the DiscardPoint that cause
	// the DiscardPoint to move regularly. For these latter one we delay applying their effect to the
	// DiscardPoint until the calculator, to save us updating them over and over.
	//
	//  - Tombstone : Accumulator
	//  - Static Metadata MaxCount 1 : Accumulator
	//  - Metadata TruncateBefore : Calculator
	//  - Metadata MaxCount : Calculator
	//  - Metadata MaxAge : Calculator
	//
	// We don't account for MaxCount in the Accumulator because every new event would cause the
	// DiscardPoint to Move. (Apart from the MaxCount1 of metadata records, since it has to persist
	// data because of the record anyway)
	//
	// We don't account for MaxAge in the Accumulator because we need the ScavengePoint to define
	// a time to calculate the age from.
	//
	// For streams that do not collide (which is ~all of them) the calculation can be done index-only.
	// that is, without hitting the log at all.
	public interface ICalculator<TStreamId> {
		void Calculate(
			ScavengePoint scavengePoint,
			IScavengeStateForCalculator<TStreamId> source,
			CancellationToken cancellationToken);

		void Calculate(
			ScavengeCheckpoint.Calculating<TStreamId> checkpoint,
			IScavengeStateForCalculator<TStreamId> source,
			CancellationToken cancellationToken);
	}

	// the chunk executor performs the actual removal of the log records
	// should be very rare to do any further lookups at this point.
	public interface IChunkExecutor<TStreamId> {
		void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken);

		void Execute(
			ScavengeCheckpoint.ExecutingChunks checkpoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			CancellationToken cancellationToken);
	}

	public interface IChunkMerger {
		void MergeChunks(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkMerger state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken);

		void MergeChunks(
			ScavengeCheckpoint.MergingChunks checkpoint,
			IScavengeStateForChunkMerger state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken);
	}

	public interface IChunkMergerBackend {
		void MergeChunks(
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken);
	}

	// the index executor performs the actual removal of the index entries
	// should be very rare to do any further lookups at this point.
	public interface IIndexExecutor<TStreamId> {
		void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForIndexExecutor<TStreamId> state,
			IIndexScavengerLog scavengerLogger,
			CancellationToken cancellationToken);

		void Execute(
			ScavengeCheckpoint.ExecutingIndex checkpoint,
			IScavengeStateForIndexExecutor<TStreamId> state,
			IIndexScavengerLog scavengerLogger,
			CancellationToken cancellationToken);
	}

	//qqq
	// - could remove certain parts of the scavenge state
	// - what pieces of information can we discard and when should we discard them
	//     - collisions (never)
	//     - hashes (never)
	//     - metastream discardpoints ???
	//     - original stream data
	//     - chunk stamp ranges for empty chunks (probably dont bother)
	//     - chunk weights
	//          - after executing a chunk (chunk executor will do this)
	public interface ICleaner {
		void Clean(
			ScavengePoint scavengePoint,
			IScavengeStateForCleaner state,
			CancellationToken cancellationToken);

		void Clean(
			ScavengeCheckpoint.Cleaning checkpoint,
			IScavengeStateForCleaner state,
			CancellationToken cancellationToken);
	}










	//
	// FOR ACCUMULATOR
	//

	//qq note dont use allreader to implement this, it has logic to deal with transactions, skips
	// epochs etc.
	public interface IChunkReaderForAccumulator<TStreamId> {
		IEnumerable<RecordForAccumulator<TStreamId>> ReadChunk(
			int logicalChunkNumber,
			ReusableObject<RecordForAccumulator<TStreamId>.OriginalStreamRecord> originalStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.MetadataStreamRecord> metadataStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.TombStoneRecord> tombStoneRecord);
	}

	//qq could use streamdata? its a class though
	public abstract class RecordForAccumulator<TStreamId>: IDisposable, IReusableObject {
		public TStreamId StreamId => _streamId;
		public long LogPosition => _basicPrepare.LogPosition;
		public DateTime TimeStamp => _basicPrepare.TimeStamp;

		private TStreamId _streamId;
		private BasicPrepareLogRecord _basicPrepare;

		public virtual void Initialize(IReusableObjectInitParams initParams) {
			var p = (RecordForAccumulatorInitParams<TStreamId>)initParams;
			_basicPrepare = p.BasicPrepare;
			_streamId = p.StreamId;
		}

		public virtual void Reset() {
			_streamId = default;
			_basicPrepare = default;
		}

		public void Dispose()
		{
			_basicPrepare?.Dispose();
		}

		public class OriginalStreamRecord : RecordForAccumulator<TStreamId> { }

		// Record in metadata stream
		public class MetadataStreamRecord : RecordForAccumulator<TStreamId> {
			public StreamMetadata Metadata {
				//qq potential for .ToArray() optimization?
				get { return _metadata ?? (_metadata = StreamMetadata.TryFromJsonBytes(_basicPrepare.Data.ToArray())); }
			}
			private StreamMetadata _metadata;
			public long EventNumber => _basicPrepare.ExpectedVersion + 1;
			public override void Reset() {
				base.Reset();
				_metadata = default;
			}
		}

		public class TombStoneRecord : RecordForAccumulator<TStreamId> {
			// old scavenge, index writer and index committer are set up to handle
			// tombstones that have abitrary event numbers, so lets handle them here
			// in case it used to be possible to create them.
			public long EventNumber => _basicPrepare.ExpectedVersion + 1;
		}
	}

	public struct RecordForAccumulatorInitParams<TStreamId> : IReusableObjectInitParams {
		public readonly BasicPrepareLogRecord BasicPrepare;
		public readonly TStreamId StreamId;
		public RecordForAccumulatorInitParams(BasicPrepareLogRecord basicPrepare, TStreamId streamId) {
			BasicPrepare = basicPrepare;
			StreamId = streamId;
		}
	}











	//
	// FOR CALCULATOR
	//

	//qq name
	public interface IIndexReaderForCalculator<TStreamId> {
		//qq maxposition  / positionlimit instead of scavengepoint?
		long GetLastEventNumber(StreamHandle<TStreamId> streamHandle, ScavengePoint scavengePoint);

		//qq maybe we can do better than allocating an array for the return
		EventInfo[] ReadEventInfoForward(
			StreamHandle<TStreamId> stream,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint);
	}










	//
	// FOR CHUNK EXECUTOR
	//

	public interface IChunkManagerForChunkExecutor<TStreamId, TChunk> {
		IChunkWriterForExecutor<TStreamId, TChunk> CreateChunkWriter(
			int chunkStartNumber,
			int chunkEndNumber);

		IChunkReaderForExecutor<TStreamId> GetChunkReaderFor(long position);

		void SwitchChunk(
			TChunk chunk,
			bool verifyHash,
			bool removeChunksWithGreaterNumbers,
			out string newFileName);
	}

	public interface IChunkReaderForExecutor<TStreamId> {
		int ChunkStartNumber { get; }
		int ChunkEndNumber { get; }
		bool IsReadOnly { get; }
		long ChunkEndPosition { get; }
		//qq this is probably just the prepares, rename accordingly?
		IEnumerable<RecordForScavenge<TStreamId>> ReadRecords();
	}

	public interface IChunkWriterForExecutor<TStreamId, TChunk> {
		TChunk WrittenChunk { get; }

		void WriteRecord(RecordForScavenge<TStreamId> record);
		//qq finalize, etc.
	}







	//
	// FOR INDEX EXECUTOR
	//

	public interface IIndexScavenger {
		void ScavengeIndex(
			long scavengePoint,
			Func<IndexEntry, bool> shouldKeep,
			IIndexScavengerLog log,
			CancellationToken cancellationToken);
	}

	public interface IChunkReaderForIndexExecutor<TStreamId> {
		bool TryGetStreamId(long position, out TStreamId streamId);
	}
















	//
	// MISC
	//

	// So that the scavenger knows where to scavenge up to
	public interface IScavengePointSource {
		// returns null when no scavenge point
		//qq rename to GetLatestOrDefault ?
		Task<ScavengePoint> GetLatestScavengePointAsync();
		Task<ScavengePoint> AddScavengePointAsync(long expectedVersion, int threshold);
	}

	// when scavenging we dont need all the data for a record
	//qq but we do need more data than this
	// but the bytes can just be bytes, in the end we are going to keep it or discard it.
	//qq recycle this record like the recordforaccumulation?
	//qq hopefully doesn't have to be a class, or can be pooled
	//qqqqq this will need some rethinking to accommodate the chunk execution logic and
	// its treatment of things like transactions. probably we will need access to the actual
	// LogRecord instance
	public class RecordForScavenge {
		public static RecordForScavenge<TStreamId> CreateScavengeable<TStreamId>(
				TStreamId streamId,
				DateTime timeStamp,
				long eventNumber,
				byte[] bytes) {

			return new RecordForScavenge<TStreamId>() {
				IsScavengable = true,
				TimeStamp = timeStamp,
				StreamId = streamId,
				EventNumber = eventNumber,
				RecordBytes = bytes,
			};
		}

		public static RecordForScavenge<TStreamId> CreateNonScavengeable<TStreamId>(
			byte[] bytes) {

			return new RecordForScavenge<TStreamId>() {
				IsScavengable = false,
				RecordBytes = bytes,
			};
		}
	}

	public class RecordForScavenge<TStreamId> {
		public bool IsScavengable { get; set; } //qq temporary messy
		public TStreamId StreamId { get; set; }
		public DateTime TimeStamp { get; set; }
		public long EventNumber { get; set; }
		//qq pool
		public byte[] RecordBytes { get; set; }
	}

	//qqq this is now IStateForChunkExecutor
	//public interface IScavengeInstructions<TStreamId> {
	//	//qqqqq is chunknumber the logical chunk number?
	//	//qq do we want to store a separate file per logical chunk or per physical (merged) chunk.
	//	IEnumerable<IReadOnlyChunkScavengeInstructions<TStreamId>> ChunkInstructionss { get; }
	//	//qq this isn't quite it, prolly need stream name
	//	bool TryGetDiscardPoint(TStreamId streamId, out DiscardPoint discardPoint);
	//}

	// Refers to a stream by name or by hash
	// This struct is json serialized, don't change the names naively
	public struct StreamHandle {
		//qq consider specifying byte if we are going to end up with a lot of these in memory
		public enum Kind {
			None,
			Hash,
			Id,
		};

		public static StreamHandle<TStreamId> ForHash<TStreamId>(ulong streamHash) {
			return new StreamHandle<TStreamId>(kind: Kind.Hash, default, streamHash);
		}

		public static StreamHandle<TStreamId> ForStreamId<TStreamId>(TStreamId streamId) {
			return new StreamHandle<TStreamId>(kind: Kind.Id, streamId, default);
		}
	}

	// Refers to a stream by name or by hash
	// this unifies the entries, some just have the hash (when we know they do not collide)
	// some contain the full stream id (when they do collide)
	//qq consider explicit layout
	public readonly struct StreamHandle<TStreamId> {

		public readonly StreamHandle.Kind Kind;
		public readonly TStreamId StreamId;
		public readonly ulong StreamHash;

		public StreamHandle(StreamHandle.Kind kind, TStreamId streamId, ulong streamHash) {
			Kind = kind;
			StreamId = streamId;
			StreamHash = streamHash;
		}

		public override string ToString() {
			switch (Kind) {
				case StreamHandle.Kind.Hash:
					return $"Hash: {StreamHash}";
				case StreamHandle.Kind.Id:
					return $"Id: {StreamId}";
				case StreamHandle.Kind.None:
				default:
					return $"None";
			};
		}
	}

	public struct MetastreamData {
		public static MetastreamData Empty { get; } = new MetastreamData(
			isTombstoned: false,
			discardPoint: DiscardPoint.KeepAll);

		public MetastreamData(
			bool isTombstoned,
			DiscardPoint discardPoint) {

			IsTombstoned = isTombstoned;
			DiscardPoint = discardPoint;
		}

		/// <summary>
		/// True when the corresponding original stream is tombstoned
		/// </summary>
		public bool IsTombstoned { get; }

		public DiscardPoint DiscardPoint { get; }
	}

	//qq according to IndexReader.GetStreamLastEventNumberCached
	// if the original stream is hard deleted then the metadatastream is treated as deleted too
	// according to IndexReader.GetStreamMetadataCached
	// the metadata for a metadatastream cannot be overwritten
	//qq so if we get a metadata FOR a metadata stream, we should ignore it.
	//qq if we get a tombstone for a metadata stream?
	//     - see how the system handles it for reads. if it ignores it we should too. if it clears the metadata we should too
	//qq for all of thes consider how much precision (and therefore bits) we need
	//qq look at the places where we construct this, are we always setting what we need
	// might want to make an explicit constructor. can we easily find the places we are calling 'with' ?
	//qq for everything in here consider signed/unsigned and the number of bits and whether it needs to
	// but nullable vs, say, using -1 to mean no value.
	//qq consider whether to make this immutable or reusable and make sure we are using it appropriately.
	public class OriginalStreamData {
		public static OriginalStreamData Empty { get; } = new OriginalStreamData(); //qq maybe dont need

		public OriginalStreamData() {
		}

		// Populated by Accumulator. Read by Calculator.
		// (MaxAge also read by ChunkExecutor)
		public long? MaxCount { get; set; }
		public TimeSpan? MaxAge { get; set; } //qq can have limited precision?
		public long? TruncateBefore { get; set; }
		public bool IsTombstoned { get; set; }

		// Populated by Calculator. Read by Calculator and Executors.
		public DiscardPoint DiscardPoint { get; set; }
		public DiscardPoint MaybeDiscardPoint { get; set; }

		public override string ToString() =>
			$"MaxCount: {MaxCount} " +
			$"MaxAge: {MaxAge} " +
			$"TruncateBefore: {TruncateBefore} " +
			$"IsTombstoned: {IsTombstoned} " +
			$"DiscardPoint: {DiscardPoint} " +
			$"MaybeDiscardPoint: {MaybeDiscardPoint} " +
			"";
	}

	// For ChunkExecutor, which implements maxAge more accurately than the index executor
	public struct ChunkExecutionInfo {
		public ChunkExecutionInfo(
			bool isTombstoned,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint,
			TimeSpan? maxAge) {

			IsTombstoned = isTombstoned;
			DiscardPoint = discardPoint;
			MaybeDiscardPoint = maybeDiscardPoint;
			MaxAge = maxAge;
		}

		public bool IsTombstoned { get; }
		public DiscardPoint DiscardPoint { get; }
		public DiscardPoint MaybeDiscardPoint { get; }
		public TimeSpan? MaxAge { get; }
	}

	public struct IndexExecutionInfo {
		public IndexExecutionInfo(
			bool isMetastream,
			bool isTombstoned,
			DiscardPoint discardPoint) {

			IsMetastream = isMetastream;
			IsTombstoned = isTombstoned;
			DiscardPoint = discardPoint;
		}

		public bool IsMetastream { get; }

		/// <summary>
		/// True when the corresponding original stream is tombstoned
		/// </summary>
		public bool IsTombstoned { get; }

		public DiscardPoint DiscardPoint { get; }
	}


	//qq implement performance overrides as necessary for this struct and others
	// (DiscardPoint, StreamHandle, ..)
	// store a range per chunk so that the calculator can definitely get a timestamp range for each event
	// that is guaranteed to to contain the real timestamp of that event.
	// if we inferred the min from the previous chunk, its possible than an incorrect clock could
	// give us an empty range for a chunk, which would be awkward to deal with in the calculator.
	public struct ChunkTimeStampRange {
		//qq reduce the precision to save persistent space.
		// say nearest minute is good enough, round min down to nearest minute and max up.
		// this is only used to control the index scavenge and to give a heuristic to the 
		// chunk scavenge so, 1 minute is probably more than plenty. look at how much resolution
		// we can get for different numbers of bytes.
		public ChunkTimeStampRange(DateTime min, DateTime max) {
			Min = min;
			Max = max;
		}

		public DateTime Min { get; }

		public DateTime Max { get; }
	}

	//qq some kind of configurable speed throttle on each of the phases to stop them hogging iops

	//qq consider, if we were happy to not scavenge streams that collide, at least for now, we could
	// get away with only storing data for non-colliding keys in the magic map.

	//qq incidentally, could any of the scavenge state maps do with bloom filters for quickly skipping
	// streams that dont need to be scavenged

	//qq some events, like empty writes, do not contain data, presumably dont take up any numbering
	// see what old scavenge does with those and what we should do with them

	//qq remove tempStream from metadata (but cope with it being serialized still)


	//=====================
	//qqqq SUBSEQUENT SCAVENGES AND RELATED QUESTIONS
	//=====================

	// EXPANDING metadatas
	// - ideally it would be nice if running a scavenge does not remove data that
	//   can currently be read by the system. otherwise it could be considered 'dangerous' to run a
	//   scavenge.
	//
	// - a user can currently _expand_ a metadata (meaning, change it so that events previously not
	//   readable become readable. this is handy because it means that setting an incorrect metadata does
	//   not cause the data to be permanently unretrievable - thats what the scavenge does.
	//
	// - this does not work terribly well with old scavenge because it can result in read errors when
	//   a scavenge has run removing some events leaving uncontiguous results.
	//
	// - but it is worse with new scavenge because there is a risk that the scavenge will remove events
	//   that can currently be read, because fundamentally scavenge only takes account of the log up to
	//   a certain point, so if the metadata was expanded after that, it wont know it.
	//      - most trivially
	//         - say there are 10 events in the stream
	//         - set the metadata to maxcount 1
	//         - start a scavenge, which calculates the appropriate discard point
	//         - expand the metadata, immediately more records can be read
	//         - the scavenge will still remove those records based on the discard point it calculated
	//           when it gets there.
	//      - or when a scavenge is started on another node, placing a scavenge point, then we expand
	//        the metadata. then the expanded metadata is not included when we scavenge this node.
	//
	// - maybe the way to explain that
	//     1. expanding the metadata is fine without using old or new scavenge
	//     2. the danger with new scavenge occurrs if you add a scavenge point (which effectively saves
	//        some metadata) and then expand the metadata before executing that scavenge point.
	//        if you execute the scavenge point first then everything that it is going to remove will be
	//        removed, so expanding the metadata wont show you anything in danger.
	//        if the goal is to recover accidentally removed events, then expand the metadata and copy
	//        them before executing the pending scavenge point
	//   this may be acceptable because expanding the metadata was always a bit janky rather than being
	//   a core feature.
	//
	// softundelete is almost a kind of expanding the metadata that we really do need to support,
	// but it isn't quite. soft-undelete expands the metadata, BUT it will never try to move the DP backwards
	//   because the DP was limited by the last event and did not advance to max. another way to say it
	//   is that soft-undelete doesn't reinstate any events that were previously scavengable

	// MOVING DISCARD POINTS BACKWARDS
	// - the question is whether a subsequent scavenge should allow the discard point to move backwards
	//   corresponding to an expanded metadata at least _attempting_ to keep more data.
	//
	// - the advantage of allowing it to move back is that (sort of rarely) it allows us to keep data
	//   that can currently be read instead of discarding it. when the metadata has a maxage, it is
	//   possible that some records that could be discarded are actually retained in the chunks
	//   (if they dont weigh enough for the threshold) and in the index (which uses the main discard
	//   point and not the maybediscardpoint). in which case moving the discard point back on a
	//   subsequent scavenge could rescue those events.
	//
	// - however it doesn't gurantee not to scavenge events that can be read, it just makes the cases
	//   more complicated, so there is limited advantage to this.
	//
	// - the disadvantage of allowing it to move back is that it is possible for events to permanently
	//   escape having weight added for them in the future meaning we have no way to guarantee they
	//   are scavenged unless we run a threshold=0 scavenge. which is a rather significant implication
	//   for gdpr. this can happen if we removed it from the index but not yet from the log, since after
	//   that the calculator wont be able to find it next time to re-add the weight.
	//      - set metadata maxcount 1. say there are events 0-10 in the stream.
	//      - run a scavenge
	//         - dp 10, weight added to chunk for events 0-9
	//         - scavenge index, remove events up to 9
	//         - no events removed from chunks because threshold not met
	//      - set metadata to maxcount 6
	//      - run a scavenge
	//         - dp moved back to 5
	//         - chunk scavenged, remove events up to 4, chunk weight reset to 0.
	//      - more events added to stream
	//      - run a scavenge
	//         - dp 10, but weight for 0-4 NOT added to the chunks because they were not in the index
	//           and therefore could not be discovered by the calculator.
	//
	// - THEREFORE we prevent the scavengepoints from being moved backwards (in an emergency they can
	//   still be moved backwards by deleting the scavenge state, or at least those discard points)
	//      - we could also mitigate this by having the reads respect the discard points if we wanted.
	//        such that expanding the metadata after calculation doesn't temporarily allow extra reads.*
	//      - or we could check the originalstreamdata when expanding the metadata and disallow it if
	//        it would move the discard points backwards (or only expand it up to that point)*
	//      - *but both of these only help if the discard points have been calculated, which they haven't
	//        necessarily
	//
	//qq we can now consider not storing the istombstoned flag, but just baking it in to the DP
	// but we cannot do this with TB, because if we allowed it to move the DP to max (for soft delete)
	// we would have to support moving it back again (for soft undelete)
	//
	//qqqq IMPORTANT: look carefully at the chunkexecutor and the index executor, see if the chunk executor
	// relies on the index at all. see if the index executor relies on the chunks at all. if so it may be
	// important to run one before the other.
	//











	//qq we should definitely log out the collisions as they are found, and log out the whole list of collisions
	// when starting a scavenge.
	//
	//qq tidying: after a scavenge there are probaly some entries in the scavenge state that we can skip
	// calculation for. for example if it was tombtoned. and perhaps tb
	// as long as the tb was fully executed. but we need to keep the discard points because the chunks
	// might not all have been executed.






	// GDPR
	// the chunk weights are only approximate because
	//   - they dont include metadata records (there arent usually many of these per stream though)
	//   - they don't include uncommitted transactions
	// 
	// therefore gdpr limitations
	//   - dont put personal data in metadata, cant guarnatee it will get scavenged
	//   - dont put personal data in transactions, it wont (and never was) be scavenged if the
	//     transaction is committed in a different chunk to when it was opened, or if it was left open
	//     because that information is not local to the chunk we are scavenging.
	//       - although we _could_ accumulate a list of open transactions it seems like its an occasional
	//         batch operation that isn't quite the same thing as the scavenge, especially as
	//         transactions are legacy and not an ongoing concern.
	//   - if the user swaps in a chunk from another node, it might contain data that we have already
	//     scavenged. //qq we need some way to figure that out and scavenge that chunk if they have strict
	//     data removal concerns.. 



	//qq perhaps scavenge points go in a scavenge point stream so they can be easily found
	// perhaps that stream should have a maxcount, but im inclined not to add one automatically for now
	// because there wont be _that_ many and the full history might be useful since its a lot of new code


	//qq consider compatibility with old scavenge
	// - config flag to choose which scavenge to run
	// - old by default
	// you definitely have to be able to run old scavenge first, thats typical.
	// will it work to run old scavenge after new scavenge? don't see why not
	// will it work to interleave them?
	/*
	 * > is it true that a stream's last event number before the scavenge point will not be scavenged?
		actually there is something to consider here
		if they put a scavenge point, wrote some more events, and then run an old scavenge and then run a new scavenge, it might not be true
		so if we need this property, we might need to add some limitation about how old/new scavenges can be run with respect to each other
	 */
	// if we want to disable the old scavenge after running the new we could
	//    - bump the chunk schema version
	//    - have old scavenge check for scavengepoints and abort?

	//qqqq need comment/plan about that flag that allows the last event to be removed.
	//qqqq need comment/plan on out of order and duplicate events

	//qq note, probably need to complain if the ptable is a 32bit table

	//qq BACKUP STRATEGY: same as current, only take a backup while scavenge is not running. this is
	// point towards not having the accumulator running continuall too. see if the backup instrutions
	// will work, and see if the cli needs modifying

	//qq dont forget about chunk merging.. maybe is that another phase after execution, or part of
	// execution.




	//qq DETERMINISTIC SCAVENGE
	// there are some interesting things to note/decide about this
	// - when we write the scavenge point, it is in the open chunk, which we can't scavenge.
	//   so at the time we write it, we cant scavenge up to the scavenge point.
	//   but if we scavenged later, we would be able to scavenge right up to it.
	//   but we want the scavenge to be deterministic, so perhaps it should always only scavenge
	//   up to the end of the chunk before the chunk containing the scavenge point.
	//   BUT what does it really mean to not scavenge the open chunk
	//     - can we accumulate the content of the open chunk?
	//     - can we calculate according to the content of the open chunk?
	//     - can we execute the index of things that are in the open chunk?
	//     - can we execute the open chunk itself?
	//    quite possibly the answer to all these except the last is yes and still keep determinism.
	//
	// - to be deterministic, we may want to the chunk weight threshold (and other things?
	//    unsafediscardtombstones?) properties of the ScavengePoint and instead of configuration options?
	//
	// - implications if the scavenge state is deleted, could we jump to the last scavenge point, or
	//   would we need to scavenge each in turn (hopefully the former)
	//
	// - new scavenge is deterministic but the current state is important, ponder these:
	//    - if old scavenge left the chunks in different states, new scavenge won't necessarily bring
	//      them in line, or will it.
	//    - if two nodes are at different points of the scavenge, then the chunks could be in different
	//      states
	//
	// - if we delete the scavenge state and want to rebuild it (say it is corrupted in a backup, say
	//   they took the backup while scavenge was running)
	//   then it would be sad if we had to rerun every scavenge point
	//   but if we can somehow jump to the last one, then perhaps a node that just hasnt been scavenged
	//   in several points could just jump to the last one also.
	//   in any of these cases getting the chunk version numbering right could be awkward, i wonder
	//   whether it is important.
	//   even if we have to jump through some hoops for the chunks, we pretty surely only need to
	//   scavenge the index once.
	//
	// - if we put the threshold in the scavenge points, perhaps to skip over scavenge points you just
	//   need to min/max the threshold.
	//
	// - if we could get a better idea of what goes wrong when you mix and match chunks today, then we
	//   could make sure that we could avoid those problems without being overly strict (like perhaps
	//   the chunk version numbering isnt important, perhaps the exact way that the chunks are merged
	//   isnt important. etc)






	//
	// todo:
	// - start creating high level tests to test the scavenger (see how the old scavenge tests work, we
	//   may be able to use them - the effect of the scavenge ought to be pretty much identical to the
	//   user but the effect on the log will be different so may not be able to use exactly the same
	//   tests (e.g. we will drop in scavenge points, might not scavenge the same things exactly that
	//   are done on a best effort basis like commit records)
	// - port the ScavengeState tests over to the higher level tests
	// - pass through the doc in case there are more things to think about
	// - want the same unit tests to run against mock implementations and real implementations of
	//      the adapter interfaces ideally
	// - implement/test the rest of the logic in the scavenge (tdd)
	//     - tidying phase
	//     - stopping/resuming
	//     - ...
	// - implement/test the adapters that plug it in to the rest of the system
	//     - includes extra apis to the index but they ought to not be too bad
	// - implement/test the persistent scavengemap
	// - integrate starting/stopping with eventstore proper
	// - performance testing
	// - forward port to master - ptables probably wont be a trivial change
	// - probably forward/back port to 20.10 and 21.10
	// - write docs
	// - 
	// ...
	//
	// dimensions for testing (perhaps in combination also)
	//   - committed transactions
	//   - uncommitted transactions
	//   - transactions crossing chunks
	//   - setting maxage/maxcount/tb
	//   - expanding metadata
	//   - contracting metadata
	//   - metadata for streams that dont exist
	//   - metadata for streams that do exist but are created after
	//   - tombstones
	//   - tombstone in transaction
	//   - metadata in transaction
	//   - tombstone in metadata stream
	//   - metadata for metadata stream
	//   - stopped/resumed scavenge
	//   - initial/subsequent scavenge
	//   - merged chunks
	//   - log record schema versions (esp wrt tombstones since it affects what the max value is)
	//   - ...
	//
	//
	//
	//qq REDACTION
	//  needs notes





	//qq TOMBSTONES
	// tldr: tombstones do not necessarily have eventnumber int.maxvalue or long.maxvalue.
	//       but they do have PrepareFlags.StreamDelete.
	//       we are therefore reluctant to just have the accumulator capture tombstones the DiscardPoint
	//       because it won't be obvious that the discard point represents a tombstone. some cases later
	//       _might_ involve moving the DiscardPoint backwards but we must not do that if it represents
	//       a tombstone. For now keep it simple and obvious and store the istombstone explicitly
	//       and _not_ in the discard point. however dont keep it for metastreams, the discard point
	//       set by the accumulator accurately describes the required effect, and there are no
	//       complications of having other metadata affect it for metadata streams.
	// 
	// Note Metadata streams cannot be tombstoned abort if we find something like this.
	//
	// Although the StorageWriterService does, these days, create tombstones with
	// EventNumber = EventNumber.DeletedStream the TFChunkScavenger, IndexComitter, and IndexWriter are
	// all geared up not to rely on that, but to rely on the prepareflags instead. It looks like they use
	// this to causes index.GetStreamLastEventNumber to return long.max (i.e. EventNumber.DeletedStream)
	// regardless of the eventnumber of the tombstone. SO lets assume that some old tombstones have other
	// eventnumbers and use the prepareflag to detect it.
	//
	// Note, if there is such a tombstone whose eventnumber is not long.max, then that means that its
	// event number in its indexentry would be different to its event number in the log.
	//
	// Therefore we can't just have the Accumulator set the DP to max on tombstone - because that might
	// discard the tombstone itself. The accumulator would have to set the DP to discard before the
	// tombstones actual position. But then it wouldn't be obvious that the DP represents a tombstone.
	//
	// For original streams lets keep it simple and set an explicit IsTombstoned flag. The calculator
	// can take that into account when calculating the discard points. Its possible we could have the
	// accumulator bake the tombstone into the DP instead to save us storing the flag, but we would then
	// have two places setting the discard points for normal streams so be careful. come back to this
	// later and we can check what places are accessing the IsTombstoned flag. Similarly for TB
	//
	//  - we can probably persist the flag in the sign bit of the discard point itself


	//qq we should definitely have more tests checking the scavenge results when there are hash collisions
	//qq we need to make sure that the calculator can find the tombstone in the index and the right thing
	// happens when it does

	//qq OLD INDEX SCAVENGE TESTS
	// - there is no need to run these tests against new scavenge.
	// - in a nutshell they test the TableIndex (and PTables), which new and old scavenge both use.
	// - they just test that the tableindex scavenge works correctly given a function that determines
	//   whether to keep each index entry. in the tests this function is injected via the fakeReader
	//   and it (for example) trivially checks if the position is in a list of deleted positions.
	//   therefore these tests are assuming a correct shouldKeep method and checking that TableIndex
	//   responds appropriately (when index upgrades, cancellation, awaiting tables etc). there would be
	//   no advantage to running them again injecting our own dummy shouldkeep implementation.
	//
	//qq make sure whenever we are cashing to prepare and accessing eventnumber that we are coping with
	// it being part of a transaction
	//qq make sure we never interact with index entries beyong the scavengepoint because we don't know
	// whether they are collisions or not

	//qq add a test that covers a chunk becoming empty on scavenge
	//qqqqq this should not care that it is persisted in a log record
	// these are json serialized in the checkpoint,
	public class ScavengePoint {
		public ScavengePoint(long position, long eventNumber, DateTime effectiveNow, int threshold) {
			Position = position;
			EventNumber = eventNumber;
			EffectiveNow = effectiveNow;
			Threshold = threshold;
		}

		// the position to scavenge up to (exclusive)
		public long Position { get; }

		public long EventNumber { get; }

		public DateTime EffectiveNow { get; }

		// The minimum a physical chunk must weigh before we will execute it.
		// Stored in the scavenge point so that (later) we could specify the threshold when
		// running the scavenge, and have it affect that scavenge on all of the nodes.
		public int Threshold { get; }

		public string GetName() => $"SP-{EventNumber}";
	}

	// These are stored in the data of the payload record
	public class ScavengePointPayload {
		public int Threshold { get; set; }

		public byte[] ToJsonBytes() =>
			Json.ToJsonBytes(this);

		public static ScavengePointPayload FromBytes(byte[] bytes) =>
			Json.ParseJson<ScavengePointPayload>(bytes);
	}
}
