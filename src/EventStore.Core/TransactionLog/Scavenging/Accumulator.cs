using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Accumulator<TStreamId> : IAccumulator<TStreamId> {
		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkReaderForAccumulator<TStreamId> _chunkReader;

		public Accumulator(
			ILongHasher<TStreamId> hasher,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkReaderForAccumulator<TStreamId> chunkReader) {

			_hasher = hasher;
			_metastreamLookup = metastreamLookup;
			_chunkReader = chunkReader;
		}

		public void Accumulate(
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state) {

			//qq todo finish in right place, which is at latest the last open chunk when we get there
			// or the scavenge point.
			//qq stop before the scavenge point, or perhaps more likely at the end of the chunk before
			// the chunk that has the scavenge point in because at the time the scavenge point is written
			// that is as far as we can scavenge up to.

			//qq todo start from right place
			var logicalChunkNumber = 0;

			try {
				while (AccumulateChunkAndRecordRange(
						scavengePoint,
						state,
						logicalChunkNumber)) {

					logicalChunkNumber++;
				}
			}
			catch {
				//qq once the tests have scavengepoints we can remove this try/catch
				// because the scavengepoints will stop the enumeration instead of
				// running off of the end of the log like this
			}
		}

		// returns true to continue
		private bool AccumulateChunkAndRecordRange(
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			int logicalChunkNumber) {

			var ret = AccumulateChunk(
				scavengePoint,
				state,
				logicalChunkNumber,
				out var chunkMinTimeStamp,
				out var chunkMaxTimeStamp);

			if (chunkMinTimeStamp <= chunkMaxTimeStamp) {
				state.SetChunkTimeStampRange(
					logicalChunkNumber: logicalChunkNumber,
					new ChunkTimeStampRange(
						min: chunkMinTimeStamp,
						max: chunkMaxTimeStamp));
			} else {
				// empty range, no need to store it.
			}

			return ret;
		}

		// returns true to continue
		// do not assume that record TimeStamps are non descending, clocks can change.
		private bool AccumulateChunk(
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			int logicalChunkNumber,
			out DateTime chunkMinTimeStamp,
			out DateTime chunkMaxTimeStamp) {

			// start with empty range and expand it as we discover records.
			chunkMinTimeStamp = DateTime.MaxValue;
			chunkMaxTimeStamp = DateTime.MinValue;

			var stopBefore = scavengePoint.Position;
			foreach (var record in _chunkReader.ReadChunk(logicalChunkNumber)) {
				if (record.LogPosition >= stopBefore) {
					return false;
				}

				if (record.TimeStamp < chunkMinTimeStamp)
					chunkMinTimeStamp = record.TimeStamp;

				if (record.TimeStamp > chunkMaxTimeStamp)
					chunkMaxTimeStamp = record.TimeStamp;

				switch (record) {
					//qq there may be other cases... the 'empty write', the system record (epoch)
					// which we might still want to get the timestamp of.
					// oh and commit records which we probably want to handle the same as prepares
					case RecordForAccumulator<TStreamId>.EventRecord x:
						ProcessEvent(x, state);
						break;
					case RecordForAccumulator<TStreamId>.MetadataRecord x:
						ProcessMetadata(x, state);
						break;
					case RecordForAccumulator<TStreamId>.TombStoneRecord x:
						ProcessTombstone(x, state);
						break;
					default:
						throw new NotImplementedException(); //qq
				}
			}

			return true;
		}

		// For every* event we need to see if its stream collides.
		// its not so bad, because we have a cache
		// * maybe not every.. beyond a certain point we only need to check records with eventnumber 0
		//    but doing so has some complications
		//      - events in transactions dont have an event number... we could just check all of these
		//      - have to identify the point at which we can switch to just checking 0s
		//      - is it possible that a new stream starts at non-zero without already having been checked
		//        before? seems unlikely.. 
		private static void ProcessEvent(
			RecordForAccumulator<TStreamId>.EventRecord record,
			IScavengeStateForAccumulator<TStreamId> state) {
			//qq hmm for transactions does this need to be the prepare log position,
			// the commit log position, or, in fact, both? it would need to be the prepare position.
			//qq can metadata be written as part of a transaction (decided will detect and abort as an
			// unsupported case)
			state.DetectCollisions(record.StreamId);
		}

		// For every metadata record
		//   - check if the stream collides
		//   - cache the metadata against the metadatastream handle.
		//         this causes scavenging of the original stream and the metadata stream.
		//qq definitely add a test that metadata records get scavenged though
		private void ProcessMetadata(
			RecordForAccumulator<TStreamId>.MetadataRecord record,
			IScavengeStateForAccumulator<TStreamId> state) {

			if (!_metastreamLookup.IsMetaStream(record.StreamId)) {
				// found metadata record that isn't in a metadata stream.
				// treat it the same as a normal event.
				state.DetectCollisions(record.StreamId);
				return;
			}

			var originalStreamId = _metastreamLookup.OriginalStreamOf(record.StreamId);
			state.DetectCollisions(originalStreamId);
			state.DetectCollisions(record.StreamId);

			if (!state.TryGetMetastreamData(record.StreamId, out var metaStreamData))
				metaStreamData = MetastreamData.Empty;

			//qqqq set the new stream data, leave the harddeleted flag alone.
			// consider if streamdata really wants to be immutable. also c# records not supported in v5
			var originalStream = _metastreamLookup.OriginalStreamOf(record.StreamId);
			var newStreamData = new MetastreamData {
				OriginalStreamHash = _hasher.Hash(originalStream),
				MaxAge = record.Metadata.MaxAge,
				MaxCount = record.Metadata.MaxCount,
				TruncateBefore = record.Metadata.TruncateBefore,
				//qq put this in IsTombstoned = metaStreamData.IsTombstoned,
				//qq probably only want to increase the discard point here, in order to respect tombstone
				// although... if there was a tombstone then this record shouldn't exist, and if it does
				// we probably want to ignore it
				//qq event number wont be set if this metadata is in a transaction,
				// unless we use teh allreader.
				DiscardPoint = DiscardPoint.DiscardBefore(record.EventNumber),
				
			};

			state.SetMetastreamData(record.StreamId, newStreamData);
		}

		// For every tombstone
		//   - check if the stream collides
		//   - set the discard point for the stream that the tombstone was found in to discard
		//     everything before the tombstone
		private void ProcessTombstone(
			RecordForAccumulator<TStreamId>.TombStoneRecord record,
			IScavengeStateForAccumulator<TStreamId> state) {

			state.DetectCollisions(record.StreamId);

			// it is possible, though maybe very unusual, to find a tombstone in a metadata stream
			if (_metastreamLookup.IsMetaStream(record.StreamId)) {
				if (!state.TryGetMetastreamData(record.StreamId, out var metaStreamData))
					metaStreamData = MetastreamData.Empty;

				var newMetaStreamData = new MetastreamData {
					OriginalStreamHash = metaStreamData.OriginalStreamHash,
					MaxAge = metaStreamData.MaxAge,
					MaxCount = metaStreamData.MaxCount,
					TruncateBefore = metaStreamData.TruncateBefore,
					//qq does the existing discard point need encorporating?
					//DiscardPoint = metaStreamData.DiscardPoint,
					DiscardPoint = DiscardPoint.DiscardBefore(record.EventNumber),
					IsTombstoned = true,
				};
				state.SetMetastreamData(record.StreamId, newMetaStreamData);
			} else {
				// get the streamData for the stream, tell it the stream is deleted
				var originalStreamData = new OriginalStreamData(
					isTombstoned: true,
					//qq does any previous maxAge here need encorporating?
					maxAge: null,
					//qq does the existing discard point need encorporating?
					discardPoint: DiscardPoint.DiscardBefore(record.EventNumber),
					//qq does the existing discard point need encorporating?
					maybeDiscardPoint: DiscardPoint.KeepAll);
				state.SetOriginalStreamData(record.StreamId, originalStreamData);
			}
		}
	}
}
