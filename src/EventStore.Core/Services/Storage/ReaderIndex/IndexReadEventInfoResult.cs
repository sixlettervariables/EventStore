using EventStore.Core.Data;

namespace EventStore.Core.Services.Storage.ReaderIndex {
	public struct IndexReadEventInfoResult {
		public readonly EventInfo[] EventInfos;

		public IndexReadEventInfoResult(EventInfo[] eventInfos) {
			EventInfos = eventInfos;
		}
	}
}
