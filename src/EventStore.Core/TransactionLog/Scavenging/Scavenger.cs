using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
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
		private readonly IIndexExecutor<TStreamId> _indexExecutor;
		private readonly IScavengePointSource _scavengePointSource;

		public Scavenger(
			IScavengeState<TStreamId> state,
			//qq might need to be factories if we need to instantiate new when calling start()
			IAccumulator<TStreamId> accumulator,
			ICalculator<TStreamId> calculator,
			IChunkExecutor<TStreamId> chunkExecutor,
			IIndexExecutor<TStreamId> indexExecutor,
			IScavengePointSource scavengePointSource) {

			_state = state;
			_accumulator = accumulator;
			_calculator = calculator;
			_chunkExecutor = chunkExecutor;
			_indexExecutor = indexExecutor;
			_scavengePointSource = scavengePointSource;
		}

		public Task RunAsync(
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			//qqqq something needs to write a scavengepoint into the log.. is that part of starting
			// a scavenge when there isn't one running? perhaps need to forward the write to the leader,
			// need to use optimistic concurrency so two nodes dont do this concurrently.

			//qq similar to TFChunkScavenger.Scavenge. consider what we want to log exactly
			return Task.Factory.StartNew(() => {
				var sw = Stopwatch.StartNew();

				ScavengeResult result = ScavengeResult.Success;
				string error = null;
				try {
					scavengerLogger.ScavengeStarted();

					StartInternal(scavengerLogger, cancellationToken);

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
			}, TaskCreationOptions.LongRunning);
		}

		private void StartInternal(
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			//qq consider exceptions (cancelled, and others)
			//qq this would come from the log so that we can stop/resume it. //qq what would :S
			//qq probably want tests for skipping over scavenge points.

			// each component can be started with its checkpoint or fresh from a given scavengepoint.
			if (!_state.TryGetCheckpoint(out var checkpoint)) {
				// start of the first scavenge (or the state was deleted)
				// get the latest scavengepoint and scavenge that.
				var scavengePoint = _scavengePointSource.GetScavengePoint();
				//qqqqq if it doesn't exist we need to optimistically drop one into the log
				AccumulateEtc(0, scavengePoint); //qqqqqqq is 0 ok in this case

			} else if (checkpoint is ScavengeCheckpoint.Done done) {
				// start of a subsequent scavenge.
				// get the latest scavengepoint and scavenge that
				//qqqq if it doesn't exist we need to optimistically drop one into the log
				//     if we fail to create one its probably because one was created in the mean time.
				//qqqq OR if the last one is the one that we have done then drop a new one into the log
				//   consider if that is what we want to do
				var scavengePoint = _scavengePointSource.GetScavengePoint();
				//qqqqq if it doesn't exist we need to optimistically drop one into the log
				AccumulateEtc(done.ScavengePoint.Position, scavengePoint); //qqqqqqq is 0 ok in this case

				//qqqqqq this has quite a lot in common with the case above, maybe refactor

			// the other cases are continuing an incomplete scavenge
			} else if (checkpoint is ScavengeCheckpoint.Accumulating accumulating) {
				_accumulator.Accumulate(accumulating, _state, cancellationToken);
				AfterAccumulation(accumulating.ScavengePoint);

			} else if (checkpoint is ScavengeCheckpoint.Calculating<TStreamId> calculating) {
				_calculator.Calculate(calculating, _state, cancellationToken);
				AfterCalculation(calculating.ScavengePoint);

			} else if (checkpoint is ScavengeCheckpoint.ExecutingChunks executingChunks) {
				_chunkExecutor.Execute(executingChunks, _state, cancellationToken);
				AfterChunkExecution(executingChunks.ScavengePoint);

			} else if (checkpoint is ScavengeCheckpoint.ExecutingIndex executingIndex) {
				_indexExecutor.Execute(executingIndex, _state, scavengerLogger, cancellationToken);
				AfterIndexExecution(executingIndex.ScavengePoint);

			} else {
				throw new Exception($"unexpected checkpoint {checkpoint}"); //qq details
			}

			void AccumulateEtc(long prev, ScavengePoint sp) {
				_accumulator.Accumulate(prev, sp, _state, cancellationToken);
				AfterAccumulation(sp);
			}

			void AfterAccumulation(ScavengePoint sp) {
				_calculator.Calculate(sp, _state, cancellationToken);
				AfterCalculation(sp);
			}

			void AfterCalculation(ScavengePoint sp) {
				_chunkExecutor.Execute(sp, _state, cancellationToken);
				AfterChunkExecution(sp);
			}

			void AfterChunkExecution(ScavengePoint sp) {
				_indexExecutor.Execute(sp, _state, scavengerLogger, cancellationToken);
				AfterIndexExecution(sp);
			}

			void AfterIndexExecution(ScavengePoint sp) {
				//qqq merge phase

				//qqq tidy phase if necessary
				// - could remove certain parts of the scavenge state
				// - what pieces of information can we discard and when should we discard them
				//     - collisions (never)
				//     - hashes (never)
				//     - metastream discardpoints ???
				//     - original stream data
				//     - chunk stamp ranges for empty chunks (probably dont bother)
				//     - chunk weights
				//          - after executing a chunk (chunk executor will do this)

				//qq check this is the right scavengepoint
				_state.BeginTransaction().Commit(new ScavengeCheckpoint.Done(sp));
			}
		}
	}
}
