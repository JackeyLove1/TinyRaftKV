package simple_raft

import (
	"bytes"
	"encoding/gob"
)

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		// baseIndex只是在偏移中使用，本质上是无效日志
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	//newLogEntries = append(newLogEntries, LogEntry{LogIndex: index,
	//	LogTerm: rf.log[index-baseIndex].LogTerm})
	//
	//for i := index + 1; i <= lastIndex; i++ {
	//	newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	//}
	newLogEntries = append(newLogEntries, rf.log[index - baseIndex:] ... )

	rf.log = newLogEntries

	// rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].LogIndex)
	e.Encode(newLogEntries[0].LogTerm)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		rf.nextIndex[server] = args.LastIncludeIndex + 1
		rf.matchIndex[server] = args.LastIncludeIndex
	}
	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.chanHeartBeat <- true
	rf.state = Follower
	rf.currentTerm = rf.currentTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.log = truncateLog(args.LastIncludeIndex, args.LastIncludeTerm, rf.log)

	rf.LastApplied = args.LastIncludeIndex
	rf.CommitIndex = args.LastIncludeIndex

	// rf.persist()

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.chanApplyMsg <- msg
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {

	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries,
		LogEntry{LogIndex: lastIncludedIndex, LogTerm: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].LogIndex == lastIncludedIndex && log[index].LogTerm == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) readSnapshot(data []byte) {

	defer rf.persist()
	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.CommitIndex = LastIncludedIndex
	rf.LastApplied = LastIncludedIndex

	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	go func() {
		rf.chanApplyMsg <- msg
	}()
}