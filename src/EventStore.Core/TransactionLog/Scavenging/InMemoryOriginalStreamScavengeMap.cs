using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryOriginalStreamScavengeMap<TKey> :
		InMemoryScavengeMap<TKey, OriginalStreamData>,
		IOriginalStreamScavengeMap<TKey> {

		public void SetTombstone(TKey key) {
			if (!TryGetValue(key, out var x))
				x = new OriginalStreamData();

			this[key] = new OriginalStreamData {
				DiscardPoint = x.DiscardPoint,
				MaybeDiscardPoint = x.MaybeDiscardPoint,
				MaxAge = x.MaxAge,
				MaxCount = x.MaxCount,
				TruncateBefore = x.TruncateBefore,

				// sqlite implementation would just set this column
				IsTombstoned = true,
			};
		}


		public void SetMetadata(TKey key, StreamMetadata metadata) {
			if (!TryGetValue(key, out var x))
				x = new OriginalStreamData();

			this[key] = new OriginalStreamData {
				MaybeDiscardPoint = x.MaybeDiscardPoint,
				DiscardPoint = x.DiscardPoint,
				IsTombstoned = x.IsTombstoned,

				// sqlite implementation would just set these column
				MaxAge = metadata.MaxAge,
				MaxCount = metadata.MaxCount,
				TruncateBefore = metadata.TruncateBefore,
			};
		}
	}
}
