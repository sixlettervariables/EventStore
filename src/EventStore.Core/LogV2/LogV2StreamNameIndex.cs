using System.Collections.Generic;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.LogV2 {
	public class LogV2StreamNameIndex :
		INameIndex<string>,
		INameIndexConfirmer<string>,
		IValueLookup<string>,
		INameLookup<string> {

		private readonly INameExistenceFilter _existenceFilter;

		public LogV2StreamNameIndex(INameExistenceFilter existenceFilter) {
			_existenceFilter = existenceFilter;
		}

		public void Dispose() {
		}

		public void InitializeWithConfirmed(INameLookup<string> source) {
		}

		public void CancelReservations() {
		}

		public void Confirm(IList<IPrepareLogRecord<string>> prepares, bool catchingUp, IIndexBackend<string> backend) {
			if (prepares.Count == 0)
				return;

			if (prepares[0].ExpectedVersion != ExpectedVersion.NoStream)
				return;

			var lastPrepare = prepares[prepares.Count - 1];

			if (catchingUp) {
				// nothing to do here
				// after the main index is caught up we will initialize the stream existence filter
			} else {
				_existenceFilter.Add(lastPrepare.EventStreamId, lastPrepare.LogPosition);
			}
		}

		public bool GetOrReserve(string streamName, out string streamId, out string createdId, out string createdName) {
			Ensure.NotNullOrEmpty(streamName, "streamName");
			streamId = streamName;
			createdId = default;
			createdName = default;

			// not adding the stream to the filter here, but this is safe because returning
			// true indicates that the stream might exist and no shortcut may be taken.
			return true;
		}

		public string LookupValue(string streamName) => streamName;

		public bool TryGetName(string value, out string name) {
			name = value;
			return true;
		}

		public bool TryGetLastValue(out string last) {
			throw new System.NotImplementedException();
		}
	}
}