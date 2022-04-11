using System;
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

				// sqlite implementation would just set these columns
				MaxAge = metadata.MaxAge,
				MaxCount = metadata.MaxCount,
				TruncateBefore = metadata.TruncateBefore,
			};
		}

		public void SetDiscardPoints(
			TKey key,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint) {

			if (!TryGetValue(key, out var x))
				//qq rather improve the iteration so that it can update the values without 
				// having to do a lookup here.
				throw new Exception("this shouldn't happen"); //qq detail

			this[key] = new OriginalStreamData {
				IsTombstoned = x.IsTombstoned,
				MaxAge = x.MaxAge,
				MaxCount = x.MaxCount,
				TruncateBefore = x.TruncateBefore,

				// sqlite implementation would just set these columns
				DiscardPoint = discardPoint,
				MaybeDiscardPoint = maybeDiscardPoint,
			};
		}

		public bool TryGetStreamExecutionDetails(TKey key, out StreamExecutionDetails details) {
			if (!TryGetValue(key, out var data)) {
				details = default;
				return false;
			}

			// sqlite implementation would just select these columns
			details = new StreamExecutionDetails(
				discardPoint: data.DiscardPoint,
				maybeDiscardPoint: data.MaybeDiscardPoint,
				maxAge: data.MaxAge);

			return true;
		}
	}
}
