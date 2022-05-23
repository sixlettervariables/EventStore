using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Core.Index;
using EventStore.Core.Messages;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.Services.Storage {
	// This abstracts the scavenge implementation away from the StorageScavenger
	interface IScavengerFactory {
		IScavenger Create(
			ClientMessage.ScavengeDatabase message,
			ITFChunkScavengerLog logger);
	}

	public class OldScavengerFactory : IScavengerFactory {
		private readonly TFChunkDb _db;
		private readonly ITableIndex _tableIndex;
		private readonly IReadIndex _readIndex;
		private readonly bool _alwaysKeepScavenged;
		private readonly bool _mergeChunks;
		private readonly bool _unsafeIgnoreHardDeletes;

		public OldScavengerFactory(
			TFChunkDb db,
			ITableIndex tableIndex,
			IReadIndex readIndex,
			bool alwaysKeepScavenged,
			bool mergeChunks,
			bool unsafeIgnoreHardDeletes) {

			Ensure.NotNull(db, "db");
			Ensure.NotNull(tableIndex, "tableIndex");
			Ensure.NotNull(readIndex, "readIndex");

			_db = db;
			_tableIndex = tableIndex;
			_readIndex = readIndex;
			_alwaysKeepScavenged = alwaysKeepScavenged;
			_mergeChunks = mergeChunks;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
		}

		public IScavenger Create(
			ClientMessage.ScavengeDatabase message,
			ITFChunkScavengerLog tfChunkScavengerLog) {

			return new OldScavenger(
				alwaysKeepScaveged: _alwaysKeepScavenged,
				mergeChunks: _mergeChunks,
				startFromChunk: message.StartFromChunk,
				tfChunkScavenger: new TFChunkScavenger(
					db: _db,
					scavengerLog: tfChunkScavengerLog,
					tableIndex: _tableIndex,
					readIndex: _readIndex,
					unsafeIgnoreHardDeletes: _unsafeIgnoreHardDeletes,
					threads: message.Threads));
		}
	}

	public class OldScavenger : IScavenger {
		private readonly bool _alwaysKeepScavenged;
		private readonly bool _mergeChunks;
		private readonly int _startFromChunk;
		private readonly TFChunkScavenger _tfChunkScavenger;

		public string ScavengeId => throw new NotImplementedException();

		public OldScavenger(
			bool alwaysKeepScaveged,
			bool mergeChunks,
			int startFromChunk,
			TFChunkScavenger tfChunkScavenger) {

			_alwaysKeepScavenged = alwaysKeepScaveged;
			_mergeChunks = mergeChunks;
			_startFromChunk = startFromChunk;
			_tfChunkScavenger = tfChunkScavenger;
		}

		public Task ScavengeAsync(CancellationToken cancellationToken) {
			return _tfChunkScavenger.Scavenge(
				alwaysKeepScavenged: _alwaysKeepScavenged,
				mergeChunks: _mergeChunks,
				startFromChunk: _startFromChunk,
				ct: cancellationToken);
		}
	}

	public class NewScavengerFactory : IScavengerFactory {
		private readonly IScavengeState<string> _state;
		private readonly IAccumulator<string> _accumulator;
		private readonly ICalculator<string> _calculator;
		private readonly IChunkExecutor<string> _chunkExecutor;
		private readonly IChunkMerger _chunkMerger;
		private readonly IIndexExecutor<string> _indexExecutor;
		private readonly ICleaner _cleaner;
		private readonly IScavengePointSource _scavengePointSource;

		public NewScavengerFactory(
			IScavengeState<string> state,
			IAccumulator<string> accumulator,
			ICalculator<string> calculator,
			IChunkExecutor<string> chunkExecutor,
			IChunkMerger chunkMerger,
			IIndexExecutor<string> indexExecutor,
			ICleaner cleaner,
			IScavengePointSource scavengePointSource) {

			_state = state;
			_accumulator = accumulator;
			_calculator = calculator;
			_chunkExecutor = chunkExecutor;
			_chunkMerger = chunkMerger;
			_indexExecutor = indexExecutor;
			_cleaner = cleaner;
			_scavengePointSource = scavengePointSource;
		}

		public IScavenger Create(
			ClientMessage.ScavengeDatabase message,
			ITFChunkScavengerLog logger) {

			return new Scavenger<string>(
				state: _state,
				accumulator: _accumulator,
				calculator: _calculator,
				chunkExecutor: _chunkExecutor,
				chunkMerger: _chunkMerger,
				indexExecutor: _indexExecutor,
				cleaner: _cleaner,
				scavengePointSource: _scavengePointSource,
				scavengerLogger: logger);
		}
	}
}
