package simple_raft

// AppendEntries
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check commit
	N := rf.CommitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex

	for i := rf.CommitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}
		if 2*num > len(rf.peers) {
			N = i
		}
	}

	if N != rf.CommitIndex {
		rf.CommitIndex = N
		rf.chanCommit <- true
	}

	// send AppendEntries
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			// check nextIndex and matchIndex to send AppendEntries or send InstallSnapshot
			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				var reply AppendEntriesReply

				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex-baseIndex+1:]))
				copy(args.Entries, rf.log[args.PrevLogIndex-baseIndex+1:])
				args.LeaderCommit = rf.CommitIndex

				go rf.sendAppendEntries(i, args, &reply)

			} else {
				var args InstallSnapshotArgs
				var reply InstallSnapshotReply

				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludeIndex = rf.log[0].LogIndex
				args.LastIncludeTerm = rf.log[0].LogTerm
				args.Data = rf.persister.snapshot
				// DPrintf("leader %v send InstallSnapshot to %v\n", rf.me, i)

				go rf.sendInstallSnapshot(i, args, &reply)
			}
		}
	}

}

func (rf *Raft) sendAppendEntries(serveId int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serveId].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()

	if ok {
		// check state and the args
		if rf.state != Leader {
			return ok
		}

		if args.Term != rf.currentTerm {
			return ok
		}

		// check reply
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		//if reply.Success {
		//	if len(args.Entries) > 0 {
		//		rf.nextIndex[serveId] = args.Entries[len(args.Entries)-1].LogIndex + 1
		//		rf.matchIndex[serveId] = rf.nextIndex[serveId] - 1
		//	} else {
		//		// HeartBeat ...
		//	}
		//} else {
		//	// rewrite wrong nextIndex
		//	rf.nextIndex[serveId] = reply.NextIndex
		//	rf.matchIndex[serveId] = reply.NextIndex - 1
		//}
		// simplify
		rf.nextIndex[serveId] = reply.NextIndex
		rf.matchIndex[serveId] = reply.NextIndex - 1
	}

	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	rf.chanHeartBeat <- true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if args.PrevLogIndex > lastIndex {
		reply.NextIndex = lastIndex + 1
		return
	}

	if args.PrevLogIndex < baseIndex {
		// skip ...
	} else {
		//  baseIndex <= args.PrevLogIndex <= lastIndex
		term := rf.log[args.PrevLogIndex-baseIndex].LogTerm
		if term != args.PrevLogTerm {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if term != rf.log[i-baseIndex].LogTerm {
					reply.NextIndex = i + 1
					break
				}
			}
			return

		} else {
			rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
			rf.log = append(rf.log, args.Entries...)
			reply.Success = true
			reply.NextIndex = rf.getLastIndex() + 1
		}
	}

	// check CommitIndex
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = Min(rf.getLastIndex(), args.LeaderCommit)
		rf.chanCommit <- true
	}

	return
}
