package simple_raft

import (
	"math/rand"
	"src/labrpc"
	"sync"
	"time"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persist
	currentTerm int
	votedFor    int
	voteCount   int
	state       State
	log         []LogEntry

	// all
	CommitIndex int
	LastApplied int

	// leader
	nextIndex  []int
	matchIndex []int

	// chans
	chanHeartBeat chan bool
	chanIsLeader  chan bool
	chanCommit    chan bool
	chanGrantVote chan bool
	chanApplyMsg  chan ApplyMsg
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		index = rf.getLastIndex() + 1
		DPrintf("raft: %d start ... ", rf.me)
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogCommand: command, LogIndex: index})
		rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// init
	rf.state = Follower
	rf.votedFor = -1
	rf.voteCount = 0
	rf.currentTerm = 0
	rf.log = append(rf.log, LogEntry{LogTerm: 0})

	rf.chanHeartBeat = make(chan bool, ChanSize)
	rf.chanCommit = make(chan bool, ChanSize)
	rf.chanGrantVote = make(chan bool, ChanSize)
	rf.chanIsLeader = make(chan bool, ChanSize)
	rf.chanApplyMsg = applyCh

	// initialize from a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.FSM()

	go rf.Commit()

	return rf
}

func (rf *Raft) FSM() {
	for {
		rand.Seed(time.Now().UnixNano())
		switch rf.state {
		case Follower:
			select {
			case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				rf.state = Candidate
				DPrintf("Time Out Follower: %v become Candidate\n", rf.me)

			case <-rf.chanGrantVote:
				DPrintf("Follower: %v vote for %v \n", rf.me, rf.votedFor)

			case <-rf.chanHeartBeat:
				// DPrintf("Follower: %v receive HeartBeat \n", rf.me)
			}

		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-time.After(time.Duration(rand.Int63()%300+510) * time.Millisecond):

			case <-rf.chanHeartBeat:
				rf.state = Follower

			case <-rf.chanIsLeader:
				rf.mu.Lock()
				rf.state = Leader
				rLen := len(rf.peers)
				rf.nextIndex = make([]int, rLen)
				rf.matchIndex = make([]int, rLen)
				for i := 0; i < rLen; i++{
					rf.nextIndex[i] = rf.getLastIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
				DPrintf("Candidate %v become the leader\n", rf.me)
			}

		case Leader:
			// DPrintf("Leader %v %v\n", rf.me, "broadcast AppendEntries .... ")
			rf.broadcastAppendEntries()
			time.Sleep(HeartBeatInterval)
		}
	}
}

func (rf *Raft) Commit() {
	for{
		select {
		case <- rf.chanCommit:
			rf.mu.Lock()
			commitIndex := rf.CommitIndex
			baseIndex := rf.log[0].LogIndex

			for i := rf.LastApplied + 1; i <= commitIndex; i++{
				msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].LogCommand}
				rf.chanApplyMsg <- msg
				DPrintf("me:%v, Commit msg %v \n", rf.me, msg)
			}

			rf.LastApplied = commitIndex
			rf.mu.Unlock()
		}
	}
}
