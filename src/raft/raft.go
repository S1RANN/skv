package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type State uint8

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type Log struct {
	Command interface{}
	Term    int
}

const ELECTION_TIMEOUT = 300 * time.Millisecond
const HEARTBEAT_INTERVAL = 100 * time.Millisecond

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	serverCount int
	lastReceive time.Time
	lastSend    []time.Time
	applyCh     chan ApplyMsg
	// applyMtx    sync.Mutex
	applyCond sync.Cond

	// persistent on all servers
	currentTerm int
	votedFor    int
	logs        []Log
	snapshot    []byte
	offset      int

	// volatile on all servers
	commitIndex int
	lastApplied int

	// volatile on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

func (s State) ColorString() string {
	switch s {
	case FOLLOWER:
		return "\033[32mFollower\033[0m"
	case CANDIDATE:
		return "\033[33mCandidate\033[0m"
	case LEADER:
		return "\033[31mLeader\033[0m"
	}
	return ""
}

func (rf *Raft) debug(format string, args ...interface{}) {
	prefix := []interface{}{rf.state.ColorString(), rf.me, rf.currentTerm}
	args = append(prefix, args...)
	DPrintf("[%v|\033[34m%v\033[0m|\033[35m%v\033[0m] "+format, args...)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := &bytes.Buffer{}
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.offset)

	rs := w.Bytes()
	rf.persister.Save(rs, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []Log
	var offset int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&offset) != nil {
		rf.mu.Lock()
		rf.debug("failed to read persist")
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.offset = offset
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.offset {
		rf.debug("already has newer snapshot, skip making snapshot")
		return
	}

	rf.debug("making snapshot at %v", index)
	rf.logs = rf.logs[index-rf.offset:]
	rf.offset = index
	rf.snapshot = snapshot

	rf.persist()
	rf.debug("finished making snapshot, logs: %v, offset=%v", rf.logs, rf.offset)

	rf.applyCond.Broadcast()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("received RequestVote: %+v", args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm

		rf.persist()
	} else if rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}

	myLastLogIndex := len(rf.logs) - 1 + rf.offset
	myLastLogTerm := rf.logs[myLastLogIndex-rf.offset].Term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(myLastLogTerm < args.LastLogTerm ||
			(myLastLogTerm == args.LastLogTerm && myLastLogIndex <= args.LastLogIndex)) {
		rf.lastReceive = time.Now()

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		rf.persist()
	}

	rf.debug("replying to RequestVote request: %+v", reply)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) checkElection() {
	rf.mu.Lock()

	if rf.state == LEADER {
		rf.mu.Unlock()
		return
	}

	electionTimeout := time.Duration(rand.Int()%300)*time.Millisecond + ELECTION_TIMEOUT
	if time.Since(rf.lastReceive) < electionTimeout {
		rf.mu.Unlock()
		return
	}

	rf.debug("reached election timeout %v, starting election", electionTimeout)

	rf.state = CANDIDATE
	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.lastReceive = time.Now()
	voteCh := make(chan bool, rf.serverCount)

	myLastLogIndex := len(rf.logs) - 1 + rf.offset
	myLastLogTerm := rf.logs[myLastLogIndex-rf.offset].Term

	args := &RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: myLastLogIndex,
		LastLogTerm:  myLastLogTerm,
	}

	for i := range rf.peers {
		if i != rf.me {
			rf.debug("sending RequestVote request to %v: %+v", i, args)
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						voteCh <- false
					} else if reply.Term < rf.currentTerm {
						rf.debug("received outdated RequestVote reply, ignoring")
						voteCh <- false
					} else {
						voteCh <- reply.VoteGranted
					}
				} else {
					voteCh <- false
				}
			}(i)
		}
	}

	rf.persist()

	rf.mu.Unlock()

	yes := 1
	no := 0
	for v := range voteCh {
		if v {
			yes++
		} else {
			no++
		}
		if yes > rf.serverCount/2 || no > rf.serverCount/2 {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != electionTerm {
		rf.debug("'s term has changed, discarded outdated votes")
		return
	}

	if yes > rf.serverCount/2 {
		rf.state = LEADER
		for i := range rf.nextIndex {
			rf.nextIndex[i] = myLastLogIndex + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = myLastLogIndex

		for i := range rf.peers {
			if i != rf.me {
				go rf.sendLogs(i)
			}
		}
		rf.debug("received %v/%v votes, is now leader", yes, rf.serverCount)
	} else {
		rf.debug("received %v/%v votes, failed the election", yes, rf.serverCount)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AEResult uint8

const (
	SUCCESS AEResult = iota
	REQUEST_WITH_LOWER_TERM
	PREV_LOG_HAS_CONFLICT_TERM
	LOG_NOT_EXISTS_AT_PREV_INDEX
	PREV_INDEX_IN_SNAPSHOT
)

type AppendEntriesReply struct {
	Term                   int
	Result                 AEResult
	ConflictTerm           int
	FirstConflictTermIndex int
	IndexAfterLastLog      int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("received AppendEntries request: %+v", args)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Result = REQUEST_WITH_LOWER_TERM
		return
	}

	rf.lastReceive = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1

		rf.persist()

		reply.Term = rf.currentTerm
	}

	if args.PrevLogIndex-rf.offset > len(rf.logs)-1 {
		reply.Result = LOG_NOT_EXISTS_AT_PREV_INDEX
		reply.IndexAfterLastLog = len(rf.logs) + rf.offset
		return
	}

	if args.PrevLogIndex-rf.offset < 0 {
		reply.Result = PREV_INDEX_IN_SNAPSHOT
		return
	}

	if rf.logs[args.PrevLogIndex-rf.offset].Term != args.PrevLogTerm {
		reply.Result = PREV_LOG_HAS_CONFLICT_TERM
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-rf.offset].Term

		i := args.PrevLogIndex
		for ; i-1-rf.offset >= 0 && rf.logs[i-1-rf.offset].Term == reply.ConflictTerm; i-- {
		}

		reply.FirstConflictTermIndex = i
		return
	}

	// entries: 	2 3
	// logs:	0 1
	// entries:   1 2 3
	// logs:	0 1
	// entries:   1
	// logs:	0 1 2
	idx := 0
	for ; idx < len(args.Entries) && idx+args.PrevLogIndex+1-rf.offset < len(rf.logs); idx++ {
		if args.Entries[idx].Term != rf.logs[idx+args.PrevLogIndex+1-rf.offset].Term {
			rf.logs = rf.logs[:idx+args.PrevLogIndex+1-rf.offset]
			break
		}
	}
	rf.logs = append(rf.logs, args.Entries[idx:]...)

	rf.persist()

	rf.debug("commitIndex: %v, logs: %v", rf.commitIndex, rf.logs)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.logs)-1+rf.offset {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1 + rf.offset
		}
		rf.debug("'s commitIndex is now %v", rf.commitIndex)
		rf.applyCond.Broadcast()
	}

	reply.Result = SUCCESS
}

func (rf *Raft) sendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	rf.lastSend[i] = time.Now()
	rf.mu.Unlock()

	return rf.peers[i].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendLogs(i int) {
	rf.mu.Lock()

	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	rf.debug("nextIndex: %v, matchIndex: %v", rf.nextIndex, rf.matchIndex)
	startIndex := rf.nextIndex[i]

	if startIndex-1 < rf.offset {
		go rf.sendSnapshot(i)
		rf.mu.Unlock()
		return
	}

	dstIndex := len(rf.logs) - 1 + rf.offset

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: startIndex - 1,
		PrevLogTerm:  rf.logs[startIndex-1-rf.offset].Term,
		Entries:      rf.logs[startIndex-rf.offset:],
		LeaderCommit: rf.commitIndex,
	}

	rf.debug("sending logs to %v: %+v", i, args)
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(i, args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1

		rf.persist()
		return
	}

	if rf.currentTerm != args.Term {
		return
	}

	switch reply.Result {
	case SUCCESS:
		rf.nextIndex[i] = dstIndex + 1
		if rf.matchIndex[i] < dstIndex {
			rf.matchIndex[i] = dstIndex

			// check whether to update commitIndex
			matchIndex := make([]int, rf.serverCount)
			copy(matchIndex, rf.matchIndex)

			sort.Ints(matchIndex)
			n := matchIndex[rf.serverCount/2]

			if n > rf.commitIndex && rf.logs[n-rf.offset].Term == rf.currentTerm {
				rf.commitIndex = n
				rf.debug("'s commitIndex is now %v", n)
				rf.applyCond.Broadcast()

				// notify followers commitIndex has been updated
				for i := range rf.peers {
					if i != rf.me {
						go rf.sendLogs(i)
					}
				}
			}
		}
	case PREV_LOG_HAS_CONFLICT_TERM:
		idx := args.PrevLogIndex

		for ; idx-rf.offset > 0 && rf.logs[idx-rf.offset].Term > reply.ConflictTerm; idx-- {
		}
		if rf.logs[idx-rf.offset].Term == reply.ConflictTerm {
			rf.nextIndex[i] = idx + 1
		} else {
			rf.nextIndex[i] = reply.FirstConflictTermIndex
		}
		go rf.sendLogs(i)
	case LOG_NOT_EXISTS_AT_PREV_INDEX:
		rf.nextIndex[i] = reply.IndexAfterLastLog
		go rf.sendLogs(i)
	case REQUEST_WITH_LOWER_TERM:
		// do nothing, just to identify this situation
	case PREV_INDEX_IN_SNAPSHOT:
		// do nothing, just to identify this situation
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("received InstallSnapshot request: lastIncludedIndex=%v, lastIncludedTerm=%v",
		args.LastIncludedIndex, args.LastIncludedTerm)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.lastReceive = time.Now()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1

		rf.persist()

		reply.Term = rf.currentTerm
	}

	if args.LastIncludedIndex <= rf.offset {
		return
	}

	rf.snapshot = args.Data

	rf.debug("before snapshot logs: %v, offset=%v", rf.logs, rf.offset)

	if args.LastIncludedIndex >= len(rf.logs)+rf.offset ||
		rf.logs[args.LastIncludedIndex-rf.offset].Term != args.LastIncludedTerm {
		rf.logs = []Log{{nil, args.LastIncludedTerm}}
	} else {
		rf.logs = rf.logs[args.LastIncludedIndex-rf.offset:]
	}

	rf.offset = args.LastIncludedIndex
	rf.debug("after snapshot logs: %v, offset=%v", rf.logs, rf.offset)

	rf.persist()

	rf.applyCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	rf.lastSend[i] = time.Now()
	rf.mu.Unlock()

	return rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendSnapshot(i int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.offset,
		LastIncludedTerm:  rf.logs[0].Term,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(i, args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1

		rf.persist()
		return
	}

	if rf.currentTerm != args.Term {
		return
	}

	rf.nextIndex[i] = len(rf.logs) - 1 + rf.offset
	if rf.matchIndex[i] < args.LastIncludedIndex {
		rf.matchIndex[i] = args.LastIncludedIndex
	}
}

func (rf *Raft) applyLogEntry() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		for rf.commitIndex > rf.lastApplied {
			if rf.lastApplied >= rf.offset {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.logs[rf.lastApplied-rf.offset].Command,
					CommandIndex:  rf.lastApplied,
					SnapshotValid: false,
					Snapshot:      []byte{},
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				rf.debug("applying log entry:%+v", rf.logs[rf.lastApplied-rf.offset])
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			} else {
				rf.lastApplied = rf.offset
				applyMsg := ApplyMsg{
					CommandValid:  false,
					Command:       nil,
					CommandIndex:  0,
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.logs[0].Term,
					SnapshotIndex: rf.offset,
				}
				rf.debug("applying snapshot, term=%v, index=%v", rf.logs[0].Term, rf.offset)
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		newLog := Log{command, term}
		index = len(rf.logs) + rf.offset
		rf.logs = append(rf.logs, newLog)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		rf.persist()
		rf.debug("appended log: %+v", newLog)
		rf.mu.Unlock()

		for i := range rf.peers {
			if i != rf.me {
				go rf.sendLogs(i)
			}
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != LEADER {
			go rf.checkElection()
		} else {
			for i, t := range rf.lastSend {
				if i != rf.me && time.Since(t) > HEARTBEAT_INTERVAL {
					go rf.sendLogs(i)
				}
			}

		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []Log{{nil, 0}}
	rf.snapshot = nil
	rf.offset = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.serverCount = len(rf.peers)
	rf.nextIndex = make([]int, rf.serverCount)
	rf.matchIndex = make([]int, rf.serverCount)
	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.mu)
	rf.lastReceive = time.Now()
	rf.lastSend = make([]time.Time, rf.serverCount)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogEntry()

	return rf
}
