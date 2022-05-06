using System.Collections;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TracingOriginalStreamScavengeMap<TKey> : IOriginalStreamScavengeMap<TKey> {
		private readonly IOriginalStreamScavengeMap<TKey> _wrapped;
		private readonly Tracer _tracer;

		public TracingOriginalStreamScavengeMap(
			IOriginalStreamScavengeMap<TKey> wrapped,
			Tracer tracer) {

			_wrapped = wrapped;
			_tracer = tracer;
		}

		public OriginalStreamData this[TKey key] { set => _wrapped[key] = value; }

		public IEnumerable<KeyValuePair<TKey, OriginalStreamData>> FromCheckpoint(TKey checkpoint) {
			return _wrapped.FromCheckpoint(checkpoint);
		}

		public IEnumerator<KeyValuePair<TKey, OriginalStreamData>> GetEnumerator() {
			return _wrapped.GetEnumerator();
		}

		public void SetDiscardPoints(
			TKey key,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint) {

			_tracer.Trace($"SetDiscardPoints({key}, {discardPoint}, {maybeDiscardPoint})");
			_wrapped.SetDiscardPoints(key, discardPoint, maybeDiscardPoint);
		}

		public void SetMetadata(TKey key, StreamMetadata metadata) {
			_wrapped.SetMetadata(key, metadata);
		}

		public void SetTombstone(TKey key) {
			_wrapped.SetTombstone(key);
		}

		public bool TryGetStreamExecutionDetails(TKey key, out StreamExecutionDetails details) {
			return _wrapped.TryGetStreamExecutionDetails(key, out details);
		}

		public bool TryGetValue(TKey key, out OriginalStreamData value) {
			return _wrapped.TryGetValue(key, out value);
		}

		public bool TryRemove(TKey key, out OriginalStreamData value) {
			return _wrapped.TryRemove(key, out value);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return ((IEnumerable)_wrapped).GetEnumerator();
		}
	}
}
