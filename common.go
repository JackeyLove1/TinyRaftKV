package simple_raft

import "time"

type State uint32

const (
	Leader State = iota
	Candidate
	Follower
)

const (
	AppendEntriesInterval = 50 * time.Millisecond
	HeartBeatInterval     = 50 * time.Millisecond
	ChanSize              = 1000
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

// RequestVote RPC
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

// InstallSnapShot RPC
type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}
