using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Scavenger {
		protected static readonly ILogger Log = LogManager.GetLoggerFor<Scavenger>();
	}

	public class Scavenger<TStreamId> : Scavenger, IScavenger {
		private readonly IScavengeState<TStreamId> _state;
		private readonly IAccumulator<TStreamId> _accumulator;
		private readonly ICalculator<TStreamId> _calculator;
		private readonly IChunkExecutor<TStreamId> _chunkExecutor;
		private readonly IChunkMerger _chunkMerger;
		private readonly IIndexExecutor<TStreamId> _indexExecutor;
		private readonly ICleaner _cleaner;
		private readonly IScavengePointSource _scavengePointSource;

		public Scavenger(
			IScavengeState<TStreamId> state,
			IAccumulator<TStreamId> accumulator,
			ICalculator<TStreamId> calculator,
			IChunkExecutor<TStreamId> chunkExecutor,
			IChunkMerger chunkMerger,
			IIndexExecutor<TStreamId> indexExecutor,
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

		public async Task RunAsync(
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			//qq similar to TFChunkScavenger.Scavenge. consider what we want to log exactly
			var sw = Stopwatch.StartNew();

			ScavengeResult result = ScavengeResult.Success;
			string error = null;
			try {
				scavengerLogger.ScavengeStarted();

				await StartInternal(scavengerLogger, cancellationToken);

			} catch (OperationCanceledException) {
				Log.Info("SCAVENGING: Scavenge cancelled.");
				result = ScavengeResult.Stopped;
			} catch (Exception exc) {
				result = ScavengeResult.Failed;
				Log.ErrorException(exc, "SCAVENGING: error while scavenging DB.");
				error = string.Format("Error while scavenging DB: {0}.", exc.Message);
				throw; //qqq probably dont want this line, just here for tests i dont want to touch atm
			} finally {
				try {
					scavengerLogger.ScavengeCompleted(result, error, sw.Elapsed);
				} catch (Exception ex) {
					Log.ErrorException(
						ex,
						"Error whilst recording scavenge completed. " +
						"Scavenge result: {result}, Elapsed: {elapsed}, Original error: {e}",
						result, sw.Elapsed, error);
				}
			}
		}

		private async Task StartInternal(
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			//qq consider exceptions (cancelled, and others)
			//qq probably want tests for skipping over scavenge points.

			// each component can be started with either
			//  (i) a checkpoint that it wrote previously (it will continue from there)
			//  (ii) fresh from a given scavengepoint
			//
			// so if we have a checkpoint from a component, start the scavenge by passing the checkpoint
			// into that component and then starting each subsequent component fresh for that
			// scavengepoint.
			//
			// otherwise, start the whole scavenge fresh from whichever scavengepoint is applicable.
			if (!_state.TryGetCheckpoint(out var checkpoint)) {
				// there is no checkpoint, so this is the first scavenge of this scavenge state
				// (not necessarily the first scavenge of this database, old scavenged may have been run
				// or new scavenges run and the scavenge state deleted)
				await StartNewAsync(prevScavengePoint: null, scavengerLogger, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.Done done) {
				// start of a subsequent scavenge.
				await StartNewAsync(prevScavengePoint: done.ScavengePoint, scavengerLogger, cancellationToken);

				// the other cases are continuing an incomplete scavenge
			} else if (checkpoint is ScavengeCheckpoint.Accumulating accumulating) {
				_accumulator.Accumulate(accumulating, _state, cancellationToken);
				AfterAccumulation(accumulating.ScavengePoint, scavengerLogger, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.Calculating<TStreamId> calculating) {
				_calculator.Calculate(calculating, _state, cancellationToken);
				AfterCalculation(calculating.ScavengePoint, scavengerLogger, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.ExecutingChunks executingChunks) {
				_chunkExecutor.Execute(executingChunks, _state, cancellationToken);
				AfterChunkExecution(executingChunks.ScavengePoint, scavengerLogger, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.MergingChunks mergingChunks) {
				_chunkMerger.MergeChunks(mergingChunks, _state, scavengerLogger, cancellationToken);
				AfterChunkMerging(mergingChunks.ScavengePoint, scavengerLogger, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.ExecutingIndex executingIndex) {
				_indexExecutor.Execute(executingIndex, _state, scavengerLogger, cancellationToken);
				AfterIndexExecution(executingIndex.ScavengePoint, cancellationToken);

			} else if (checkpoint is ScavengeCheckpoint.Cleaning cleaning) {
				_cleaner.Clean(cleaning, _state, cancellationToken);
				AfterCleaning(cleaning.ScavengePoint);

			} else {
				throw new Exception($"Unexpected checkpoint: {checkpoint}");
			}
		}

		private async Task StartNewAsync(
			ScavengePoint prevScavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			// prevScavengePoint is the previous one that was completed
			// latestScavengePoint is the latest one in the database
			// nextScavengePoint is the one we are about to scavenge up to

			// threshold < 0: execute all chunks, even those with no weight
			// threshold = 0: execute all chunks with weight greater than 0
			// threshold > 0: execute all chunks above a certain weight
			var threshold = 0;
			ScavengePoint nextScavengePoint;
			var latestScavengePoint = await _scavengePointSource.GetLatestScavengePointOrDefaultAsync();
			if (latestScavengePoint == null) {
				// no latest scavenge point, create the first one
				nextScavengePoint = await _scavengePointSource
					.AddScavengePointAsync(ExpectedVersion.NoStream, threshold: threshold);
			} else {
				// got the latest scavenge point
				if (prevScavengePoint == null ||
					prevScavengePoint.EventNumber < latestScavengePoint.EventNumber) {
					// the latest scavengepoint is suitable
					nextScavengePoint = latestScavengePoint;
				} else {
					// the latest scavengepoint is the prev scavenge point, so create a new one
					//qq test that if this fails the scavenge terminates with an error
					nextScavengePoint = await _scavengePointSource
						.AddScavengePointAsync(prevScavengePoint.EventNumber, threshold: threshold);
				}
			}

			// we now have a nextScavengePoint.
			_accumulator.Accumulate(prevScavengePoint, nextScavengePoint, _state, cancellationToken);
			AfterAccumulation(nextScavengePoint, scavengerLogger, cancellationToken);
		}

		private void AfterAccumulation(
			ScavengePoint scavengepoint,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			_calculator.Calculate(scavengepoint, _state, cancellationToken);
			AfterCalculation(scavengepoint, scavengerLogger, cancellationToken);
		}

		private void AfterCalculation(
			ScavengePoint scavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			_chunkExecutor.Execute(scavengePoint, _state, cancellationToken);
			AfterChunkExecution(scavengePoint, scavengerLogger, cancellationToken);
		}

		private void AfterChunkExecution(
			ScavengePoint scavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			_chunkMerger.MergeChunks(scavengePoint, _state, scavengerLogger, cancellationToken);
			AfterChunkMerging(scavengePoint, scavengerLogger, cancellationToken);
		}

		private void AfterChunkMerging(
			ScavengePoint scavengePoint,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			_indexExecutor.Execute(scavengePoint, _state, scavengerLogger, cancellationToken);
			AfterIndexExecution(scavengePoint, cancellationToken);
		}

		private void AfterIndexExecution(
			ScavengePoint scavengePoint,
			CancellationToken cancellationToken) {

			_cleaner.Clean(scavengePoint, _state, cancellationToken);
			AfterCleaning(scavengePoint);
		}

		private void AfterCleaning(ScavengePoint scavengePoint) {
			//qq check this is the right scavengepoint
			_state.SetCheckpoint(new ScavengeCheckpoint.Done(scavengePoint));
		}
	}
}
