using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.Services.Storage {
	//qq name
	public abstract class NewStorageScavenger {
	}

	//qq consider refactoring with StorageScavenger once this is more fleshed out
	//qq name, especailly wrt the wrapped scanvenger. perhaps this is the scavenger service.
	public class NewStorageScavenger<TStreamId> :
		NewStorageScavenger,
		IHandle<SystemMessage.StateChangeMessage>,
		IHandle<ClientMessage.ScavengeDatabase>,
		IHandle<ClientMessage.StopDatabaseScavenge> {

		private readonly IScavenger _scavenger;
		private readonly ITFChunkScavengerLogManager _logManager;

		public NewStorageScavenger(
			ITFChunkScavengerLogManager logManager,
			IScavenger scavenger) {

			Ensure.NotNull(scavenger, nameof(scavenger));
			Ensure.NotNull(logManager, nameof(logManager));

			_scavenger = scavenger;
			_logManager = logManager;
		}

		public void Handle(SystemMessage.StateChangeMessage message) {
			if (message.State == VNodeState.Master || message.State == VNodeState.Slave) {
				_logManager.Initialise();
			}
		}

		public void Handle(ClientMessage.ScavengeDatabase message) {
			_scavenger.Start(_logManager.CreateLog());
		}

		public void Handle(ClientMessage.StopDatabaseScavenge message) {
			_scavenger.Stop();
		}
	}
}
