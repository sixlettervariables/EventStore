namespace EventStore.Core.TransactionLog.Scavenging {
	//qq sort though these comments and delete this file


	//qq consider compatibility with old scavenge
	// - config flag to choose which scavenge to run
	// - old by default
	// you definitely have to be able to run old scavenge first, thats typical.
	// will it work to run old scavenge after new scavenge? don't see why not
	// will it work to interleave them?
	/*
	 * > is it true that a stream's last event number before the scavenge point will not be scavenged?
		actually there is something to consider here
		if they put a scavenge point, wrote some more events, and then run an old scavenge and then run a new scavenge, it might not be true
		so if we need this property, we might need to add some limitation about how old/new scavenges can be run with respect to each other
	 */
	// if we want to disable the old scavenge after running the new we could
	//    - bump the chunk schema version
	//    - have old scavenge check for scavengepoints and abort?

	//qq note, probably need to complain if the ptable is a 32bit table... why?








	//
	// todo:
	// - start creating high level tests to test the scavenger (see how the old scavenge tests work, we
	//   may be able to use them - the effect of the scavenge ought to be pretty much identical to the
	//   user but the effect on the log will be different so may not be able to use exactly the same
	//   tests (e.g. we will drop in scavenge points, might not scavenge the same things exactly that
	//   are done on a best effort basis like commit records)
	// - port the ScavengeState tests over to the higher level tests
	// - pass through the doc in case there are more things to think about
	// - want the same unit tests to run against mock implementations and real implementations of
	//      the adapter interfaces ideally
	// - implement/test the rest of the logic in the scavenge (tdd)
	//     - tidying phase
	//     - stopping/resuming
	//     - ...
	// - implement/test the adapters that plug it in to the rest of the system
	//     - includes extra apis to the index but they ought to not be too bad
	// - implement/test the persistent scavengemap
	// - integrate starting/stopping with eventstore proper
	// - performance testing
	// - forward port to master - ptables probably wont be a trivial change
	// - probably forward/back port to 20.10 and 21.10
	// - write docs
	// - 
	// ...
	//
	// dimensions for testing (perhaps in combination also)
	//   - expanding metadata
	//   - contracting metadata
	//   - metadata for streams that dont exist
	//   - metadata for streams that do exist but are created after
	//   - tombstones
	//   - merged chunks
	//   - log record schema versions (esp wrt tombstones since it affects what the max value is)
	//   - ...
	//
	//
	//







	//qq OLD INDEX SCAVENGE TESTS
	// - there is no need to run these tests against new scavenge.
	// - in a nutshell they test the TableIndex (and PTables), which new and old scavenge both use.
	// - they just test that the tableindex scavenge works correctly given a function that determines
	//   whether to keep each index entry. in the tests this function is injected via the fakeReader
	//   and it (for example) trivially checks if the position is in a list of deleted positions.
	//   therefore these tests are assuming a correct shouldKeep method and checking that TableIndex
	//   responds appropriately (when index upgrades, cancellation, awaiting tables etc). there would be
	//   no advantage to running them again injecting our own dummy shouldkeep implementation.
	//

	//qq make sure whenever we are accessing eventnumber that we are coping with
	// it being part of a transaction (i.e. eventnumber < 0)

	//qq make sure we never interact with index entries beyong the scavengepoint because we don't know
	// whether they are collisions or not

	//qq we should definitely have more tests checking the scavenge results when there are hash collisions


	//qqqq ALWAYS KEEP SCAVENGED
	// - for a threshold=0 scavenge
	//     - we only execute chunks with something to remove, and we need to remove it. dont keep the original.
	// - for a threshold>0 scavenge
	//     - we only execute chunks with something to remove, and we need to remove it. dont keep the original.
	// - for a threshold<0 scavenge
	//     - we will execute all the chunks, if there was anything to remove then we must remove it
	//     - but if there was nothing to remove then, optionally?, delete the new chunk.

	// - if a chunk has nothing to remove then make sure we don't write a posmap for it
	//   see if old scavenge does anything special here.

	// - if a chunk is bigger after being scavenged
	//     - perhaps we should keep the old, but then
	//       what should we do with the weight, halve it? but only if the thresold is positive, because
	//       if it is 0 then we need to keep the new chunk for gdpr. so this is probably an optimisation
	//       we can add later.

	// - with unsafeignoreharddeletes we have to keep the scavenged chunk even if bigger

}
