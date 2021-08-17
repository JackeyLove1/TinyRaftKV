package simple_raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// utils

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

func (msg *ApplyMsg) String() string {
	return fmt.Sprintf("Index: %v, Command: %v, UseSnapshot: %v",
		msg.Index, msg.Command, msg.UseSnapshot)
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
