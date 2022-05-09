using System;
using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Helpers;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Core.Services.UserManagement;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ScavengePointSource : IScavengePointSource {
		private readonly IODispatcher _ioDispatcher;

		public ScavengePointSource(IODispatcher ioDispatcher) {
			_ioDispatcher = ioDispatcher;
		}

		//qq wip
		public async Task<ScavengePoint> GetLatestScavengePointAsync() {
			var readTcs = new TaskCompletionSource<ResolvedEvent[]>();
			var endStreamPosition = -1;

			_ioDispatcher.ReadBackward(
				streamId: SystemStreams.ScavengePointsStream,
				fromEventNumber: endStreamPosition,
				maxCount: 1,
				resolveLinks: false,
				principal: SystemAccount.Principal,
				action: m => {
					if (m.Result == ReadStreamResult.Success)
						readTcs.TrySetResult(m.Events);
					else {
						readTcs.TrySetException(new Exception(
							//qq detail
							$"Could not read newly created scavenge point: {m.Result}. {m.Error}"));
					}
				});

			var events = await readTcs.Task;

			if (events.Length != 1) {
				throw new Exception($"Expected 1 event but got {events.Length}");
			}

			var scavengePointEvent = events[0].Event;
			var scavengePointPayload = ScavengePointPayload.FromBytes(scavengePointEvent.Data);

			var scavengePoint = new ScavengePoint(
				position: scavengePointEvent.LogPosition,
				eventNumber: scavengePointEvent.EventNumber,
				effectiveNow: scavengePointEvent.TimeStamp,
				threshold: scavengePointPayload.Threshold);

			return scavengePoint;
		}

		//qqq check this and test it, especially on a cluster
		public async Task<ScavengePoint> AddScavengePointAsync(long expectedVersion, int threshold) {
			var payload = new ScavengePointPayload {
				Threshold = threshold,
			};
			//qqq for that matter perhaps the scavengepoint stream itself should have metadata set

			//qq do these calls automatically timeout, or might they hang? old scavenge uses them to
			// log, but perhaps that is less critical
			var writeTcs = new TaskCompletionSource<bool>();
			_ioDispatcher.WriteEvent(
				streamId: SystemStreams.ScavengePointsStream,
				expectedVersion: expectedVersion,
				@event: new Event(
					eventId: Guid.NewGuid(),
					eventType: SystemEventTypes.ScavengePoint,
					isJson: true,
					data: payload.ToJsonBytes(),
					metadata: null),
				principal: SystemAccount.Principal,
				action: m => {
					if (m.Result == OperationResult.Success) {
						writeTcs.TrySetResult(true);
					} else {
						writeTcs.TrySetException(new Exception(
							//qq detail
							$"Couldn't create a scavenge point: {m.Result}"));
						//qq retry?
						//qq log an error. in fact, lots of logging everywhere.
					}
				}
			);

			await writeTcs.Task;

			var scavengePoint = await GetLatestScavengePointAsync();

			if (scavengePoint.EventNumber != expectedVersion + 1)
				throw new Exception("dfglskjas"); //qq detail

			return scavengePoint;
		}
	}
}
