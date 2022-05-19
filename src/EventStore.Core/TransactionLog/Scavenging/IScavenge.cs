namespace EventStore.Core.TransactionLog.Scavenging {
	//qq sort though these comments and delete this file
	//qq some events, like empty writes, do not contain data, presumably dont take up any numbering
	// see what old scavenge does with those and what we should do with them


	//=====================
	//qqqq SUBSEQUENT SCAVENGES AND RELATED QUESTIONS
	//=====================

	// EXPANDING metadatas
	// - ideally it would be nice if running a scavenge does not remove data that
	//   can currently be read by the system. otherwise it could be considered 'dangerous' to run a
	//   scavenge.
	//
	// - a user can currently _expand_ a metadata (meaning, change it so that events previously not
	//   readable become readable. this is handy because it means that setting an incorrect metadata does
	//   not cause the data to be permanently unretrievable - thats what the scavenge does.
	//
	// - this does not work terribly well with old scavenge because it can result in read errors when
	//   a scavenge has run removing some events leaving uncontiguous results.
	//
	// - but it is worse with new scavenge because there is a risk that the scavenge will remove events
	//   that can currently be read, because fundamentally scavenge only takes account of the log up to
	//   a certain point, so if the metadata was expanded after that, it wont know it.
	//      - most trivially
	//         - say there are 10 events in the stream
	//         - set the metadata to maxcount 1
	//         - start a scavenge, which calculates the appropriate discard point
	//         - expand the metadata, immediately more records can be read
	//         - the scavenge will still remove those records based on the discard point it calculated
	//           when it gets there.
	//      - or when a scavenge is started on another node, placing a scavenge point, then we expand
	//        the metadata. then the expanded metadata is not included when we scavenge this node.
	//
	// - maybe the way to explain that
	//     1. expanding the metadata is fine without using old or new scavenge
	//     2. the danger with new scavenge occurrs if you add a scavenge point (which effectively saves
	//        some metadata) and then expand the metadata before executing that scavenge point.
	//        if you execute the scavenge point first then everything that it is going to remove will be
	//        removed, so expanding the metadata wont show you anything in danger.
	//        if the goal is to recover accidentally removed events, then expand the metadata and copy
	//        them before executing the pending scavenge point
	//   this may be acceptable because expanding the metadata was always a bit janky rather than being
	//   a core feature.
	//
	// softundelete is almost a kind of expanding the metadata that we really do need to support,
	// but it isn't quite. soft-undelete expands the metadata, BUT it will never try to move the DP backwards
	//   because the DP was limited by the last event and did not advance to max. another way to say it
	//   is that soft-undelete doesn't reinstate any events that were previously scavengable

	// MOVING DISCARD POINTS BACKWARDS
	// - the question is whether a subsequent scavenge should allow the discard point to move backwards
	//   corresponding to an expanded metadata at least _attempting_ to keep more data.
	//
	// - the advantage of allowing it to move back is that (sort of rarely) it allows us to keep data
	//   that can currently be read instead of discarding it. when the metadata has a maxage, it is
	//   possible that some records that could be discarded are actually retained in the chunks
	//   (if they dont weigh enough for the threshold) and in the index (which uses the main discard
	//   point and not the maybediscardpoint). in which case moving the discard point back on a
	//   subsequent scavenge could rescue those events.
	//
	// - however it doesn't gurantee not to scavenge events that can be read, it just makes the cases
	//   more complicated, so there is limited advantage to this.
	//
	// - the disadvantage of allowing it to move back is that it is possible for events to permanently
	//   escape having weight added for them in the future meaning we have no way to guarantee they
	//   are scavenged unless we run a threshold=0 scavenge. which is a rather significant implication
	//   for gdpr. this can happen if we removed it from the index but not yet from the log, since after
	//   that the calculator wont be able to find it next time to re-add the weight.
	//      - set metadata maxcount 1. say there are events 0-10 in the stream.
	//      - run a scavenge
	//         - dp 10, weight added to chunk for events 0-9
	//         - scavenge index, remove events up to 9
	//         - no events removed from chunks because threshold not met
	//      - set metadata to maxcount 6
	//      - run a scavenge
	//         - dp moved back to 5
	//         - chunk scavenged, remove events up to 4, chunk weight reset to 0.
	//      - more events added to stream
	//      - run a scavenge
	//         - dp 10, but weight for 0-4 NOT added to the chunks because they were not in the index
	//           and therefore could not be discovered by the calculator.
	//
	// - THEREFORE we prevent the scavengepoints from being moved backwards (in an emergency they can
	//   still be moved backwards by deleting the scavenge state, or at least those discard points)
	//      - we could also mitigate this by having the reads respect the discard points if we wanted.
	//        such that expanding the metadata after calculation doesn't temporarily allow extra reads.*
	//      - or we could check the originalstreamdata when expanding the metadata and disallow it if
	//        it would move the discard points backwards (or only expand it up to that point)*
	//      - *but both of these only help if the discard points have been calculated, which they haven't
	//        necessarily
	//
	//
	//qqqq IMPORTANT: look carefully at the chunkexecutor and the index executor, see if the chunk executor
	// relies on the index at all. see if the index executor relies on the chunks at all. if so it may be
	// important to run one before the other.
	//











	//qq we should definitely log out the collisions as they are found, and log out the whole list of collisions
	// when starting a scavenge.
	//





	//qq perhaps scavenge points go in a scavenge point stream so they can be easily found
	// perhaps that stream should have a maxcount, but im inclined not to add one automatically for now
	// because there wont be _that_ many and the full history might be useful since its a lot of new code


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

	//qqqq need comment/plan on out of order and duplicate events

	//qq note, probably need to complain if the ptable is a 32bit table... why?






	//qq DETERMINISTIC SCAVENGE
	// there are some interesting things to note/decide about this
	// - when we write the scavenge point, it is in the open chunk, which we can't scavenge.
	//   so at the time we write it, we cant scavenge up to the scavenge point.
	//   but if we scavenged later, we would be able to scavenge right up to it.
	//   but we want the scavenge to be deterministic, so perhaps it should always only scavenge
	//   up to the end of the chunk before the chunk containing the scavenge point.
	//   BUT what does it really mean to not scavenge the open chunk
	//     - can we accumulate the content of the open chunk?
	//     - can we calculate according to the content of the open chunk?
	//     - can we execute the index of things that are in the open chunk?
	//     - can we execute the open chunk itself?
	//    quite possibly the answer to all these except the last is yes and still keep determinism.
	//
	// - to be deterministic, we may want to the chunk weight threshold (and other things?
	//    unsafediscardtombstones?) properties of the ScavengePoint and instead of configuration options?
	//
	// - implications if the scavenge state is deleted, could we jump to the last scavenge point, or
	//   would we need to scavenge each in turn (hopefully the former)
	//
	// - new scavenge is deterministic but the current state is important, ponder these:
	//    - if old scavenge left the chunks in different states, new scavenge won't necessarily bring
	//      them in line, or will it.
	//    - if two nodes are at different points of the scavenge, then the chunks could be in different
	//      states
	//
	// - if we delete the scavenge state and want to rebuild it (say it is corrupted in a backup, say
	//   they took the backup while scavenge was running)
	//   then it would be sad if we had to rerun every scavenge point
	//   but if we can somehow jump to the last one, then perhaps a node that just hasnt been scavenged
	//   in several points could just jump to the last one also.
	//   in any of these cases getting the chunk version numbering right could be awkward, i wonder
	//   whether it is important.
	//   even if we have to jump through some hoops for the chunks, we pretty surely only need to
	//   scavenge the index once.
	//
	// - if we put the threshold in the scavenge points, perhaps to skip over scavenge points you just
	//   need to min/max the threshold.
	//
	// - if we could get a better idea of what goes wrong when you mix and match chunks today, then we
	//   could make sure that we could avoid those problems without being overly strict (like perhaps
	//   the chunk version numbering isnt important, perhaps the exact way that the chunks are merged
	//   isnt important. etc)






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
	//   - committed transactions
	//   - uncommitted transactions
	//   - transactions crossing chunks
	//   - setting maxage/maxcount/tb
	//   - expanding metadata
	//   - contracting metadata
	//   - metadata for streams that dont exist
	//   - metadata for streams that do exist but are created after
	//   - tombstones
	//   - tombstone in transaction
	//   - metadata in transaction
	//   - tombstone in metadata stream
	//   - metadata for metadata stream
	//   - stopped/resumed scavenge
	//   - initial/subsequent scavenge
	//   - merged chunks
	//   - log record schema versions (esp wrt tombstones since it affects what the max value is)
	//   - ...
	//
	//
	//







	//qq we should definitely have more tests checking the scavenge results when there are hash collisions

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

	//qq dont forget to consider whether to keep unscavenged chunks or not.

	//qq add a test that covers a chunk becoming empty on scavenge

	//qqqq ALWAYS KEEP SCAVENGED
	// - if a chunk is bigger after being scavenged, perhaps we should keep the old, but then
	// what should we do with the weight, halve it? but only if the thresold is positive, because
	// if it is 0 then we need to keep the new chunk for gdpr. so this is probably an optimisation
	// we can add later.
	// - with unsafeignoreharddeletes we have to keep the scavenged chunk even if bigger
	// - what if the user runs a threshold=-1 scavenge, so we scavenge chunks that actually have
	// nothing to remove. what does old scavenge do with those chunks, if it does something efficient
	// then perhaps we should too.
}
