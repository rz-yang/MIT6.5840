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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

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

const (
	Follower int = iota
	Candidate
	Leader
)

const (
	MinElectionTimeOut  int = 250
	MaxElectionTimeOut  int = 500
	HeartbeatPeriodTime int = 150
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int // 0 for follower 1 for candidate 2 for leader

	//persistent state
	currentTerm int        // last term server has seen
	votedFor    int        // nil for none, or choose one in a term
	log         []LogEntry // log entries

	// volatile state

	// servers' side
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry known to be applied

	// leaders' side
	nextIndex  []int // index of next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server

	heartBeatFlag bool
	// heartBeatCnt int
	heartBeatMu sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// invoked by candidates to gather votes
	Term         int // candidate's term
	CandidateId  int // who request
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int  // currentTerm of server, candidate use this for update
	VoteGrant bool // true for yes
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convertFollower()
	}
	if rf.currentTerm <= args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGrant = true
		rf.votedFor = args.CandidateId
	}
	rf.mu.Unlock()
	fmt.Printf("server %v grant %v for candidate %v's requestVote\n", rf.me, reply.VoteGrant, args.CandidateId)
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

// appendEntries RPC
type AppendEntryArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id
	PrevLogIndex int        // index of which preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}
type AppendEntryReply struct {
	Term    int  // currentTerm
	Success bool // true if matched
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convertFollower()
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if rf.state == Candidate {
		rf.convertFollower()
	}
	rf.heartBeatFlag = true
	rf.mu.Unlock()
	// go rf.heartBeatReceived()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeatReceived() {
	rf.heartBeatMu.Lock()
	// rf.heartBeatCnt++
	rf.heartBeatMu.Unlock()
	time.Sleep(getDuration(getBoundRand(MinElectionTimeOut, MaxElectionTimeOut)))
	rf.heartBeatMu.Lock()
	// rf.heartBeatCnt--
	rf.heartBeatMu.Unlock()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//term, isleader := rf.GetState()
		//fmt.Printf("server %v term %v state %v(leader %v)(heartFlag %v)\n", rf.me, term, rf.state, isleader, rf.heartBeatFlag)

		// Your code here (2A)
		// Check if a leader election should be started.
		// 没收到心跳，且是Follower
		// rf.heartBeatMu.Lock()
		if rf.check() {
			// rf.heartBeatMu.Unlock()
			//fmt.Printf("server %v start election\n", rf.me)
			rf.startElection()
		}
		rf.mu.Lock()
		rf.heartBeatFlag = false
		// rf.heartBeatMu.Unlock()
		rf.mu.Unlock()
		// wait for an election timeout
		time.Sleep(getDuration(getBoundRand(MinElectionTimeOut, MaxElectionTimeOut)))
	}
}

func (rf *Raft) check() bool {

	rf.mu.Lock()
	flag := !rf.heartBeatFlag && rf.state == Follower
	rf.mu.Unlock()
	return flag
}

func (rf *Raft) startElection() {
	for {
		rf.mu.Lock()
		rf.convertCandidate()
		// send RequestVote RPCs to all other servers
		cnt := 1
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				request := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &request, &reply)
				rf.mu.Lock()
				if ok {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertFollower()
						// 退出选举
					}
					if reply.VoteGrant {
						if request.Term == rf.currentTerm {
							cnt++
						}
						if rf.state == Candidate && cnt > len(rf.peers)/2 {
							rf.convertLeader()
						}
					}
				}
				rf.mu.Unlock()
			}(i)
		}
		time.Sleep(getDuration(getBoundRand(MinElectionTimeOut, MaxElectionTimeOut)))
		if rf.getState() != Candidate {
			return
		}
	}
}

func (rf *Raft) convertFollower() {
	fmt.Printf("server %v convert to follower\n", rf.me)
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) convertCandidate() {
	fmt.Printf("server %v convert to candidate\n", rf.me)
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
}

func (rf *Raft) convertLeader() {
	fmt.Printf("server %v convert to leader\n", rf.me)
	rf.state = Leader
	rf.votedFor = -1
	go rf.leaderSendAppendEntries()
}

func (rf *Raft) getState() int {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	return state

}
func (rf *Raft) leaderSendAppendEntries() {
	for rf.getState() == Leader {
		for i := 0; i < len(rf.peers); i++ {
			go func(i int) {
				reply := AppendEntryReply{}
				rf.mu.Lock()
				term := rf.currentTerm
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(i, &AppendEntryArgs{Term: term}, &reply)
				if ok {

				}
			}(i)
		}
		time.Sleep(getDuration(HeartbeatPeriodTime))
	}
}

func getBoundRand(l, r int) int {
	return l + rand.Int()%(r-l+1)
}

func getDuration(t int) time.Duration {
	return time.Duration(t) * time.Millisecond
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

	// Your initialization code here (2A, 2B, 2C).
	cnt := len(peers)
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		state:         Follower,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]LogEntry, 1), // first index is 1
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, cnt),
		matchIndex:    make([]int, cnt),
		heartBeatFlag: false,
		// heartBeatCnt: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
