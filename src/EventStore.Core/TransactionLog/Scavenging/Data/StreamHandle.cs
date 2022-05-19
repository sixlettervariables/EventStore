namespace EventStore.Core.TransactionLog.Scavenging {
	// Refers to a stream by name or by hash
	// This struct is json serialized, don't change the names naively
	//qq: consider making this not stream specific
	public struct StreamHandle {
		public enum Kind : byte {
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
}
