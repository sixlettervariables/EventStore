using System;
using System.Collections.Generic;
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

				// sqlite implementation would insert the record or update these columns
				Status = CalculationStatus.Active, //qqq sqlite to do this
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

				// sqlite implementation would insert the record or update these columns
				Status = CalculationStatus.Active, //qqq sqlite to do this
				MaxAge = metadata.MaxAge,
				MaxCount = metadata.MaxCount,
				TruncateBefore = metadata.TruncateBefore,
			};
		}

		public void SetDiscardPoints(
			TKey key,
			CalculationStatus status,
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

				// sqlite implementation would insert the record or update these columns
				Status = status,
				DiscardPoint = discardPoint,
				MaybeDiscardPoint = maybeDiscardPoint,
			};
		}

		public bool TryGetChunkExecutionInfo(TKey key, out ChunkExecutionInfo info) {
			if (!TryGetValue(key, out var data)) {
				info = default;
				return false;
			}

			// sqlite implementation would just select these columns
			info = new ChunkExecutionInfo(
				isTombstoned: data.IsTombstoned,
				discardPoint: data.DiscardPoint,
				maybeDiscardPoint: data.MaybeDiscardPoint,
				maxAge: data.MaxAge);

			return true;
		}

		protected override bool Filter(KeyValuePair<TKey, OriginalStreamData> kvp) => 
			//qqqqq implement in sqlite with a where in the enumeration
			kvp.Value.Status == CalculationStatus.Active;
			
		public void DeleteMany(bool deleteArchived) {
			foreach (var kvp in AllRecords()) {
				if ((kvp.Value.Status == CalculationStatus.Spent) ||
					(kvp.Value.Status == CalculationStatus.Archived && deleteArchived)) {

					TryRemove(kvp.Key, out _);
				}
			}
		}
	}
}
