using System;
using EventStore.Core.Data;
using EventStore.Core.Helpers;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public abstract class RecordForAccumulator<TStreamId> : IDisposable, IReusableObject {
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

		// Record in original stream
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
}
