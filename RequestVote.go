package simple_raft

// RequestVote
func (rf *Raft) broadcastRequestVote() {

	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			var reply RequestVoteReply
			go rf.sendRequestVote(i, args, &reply)
		}
	}

}

func (rf *Raft) sendRequestVote(serverId int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[serverId].Call("Raft.RequestVote", args, reply)

	// check reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if ok {

		if rf.state != Candidate {
			return ok
		}

		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 && rf.state == Candidate{
				rf.chanIsLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	reply.Term = args.Term
	update := false

	if args.LastLogTerm > term {
		update = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index {
		update = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.chanGrantVote <- true
	}

	return
}
