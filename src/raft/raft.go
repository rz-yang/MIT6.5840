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
	"bytes"
	"fmt"
	"os"
	"raft_LSMTree-based_KVStore/labgob"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"raft_LSMTree-based_KVStore/labrpc"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Log struct {
	Entries           []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (log *Log) check(i int) bool {
	return i > log.LastIncludedIndex && i <= log.getLastIndex()
}

func (log *Log) beginIndex() int {
	return log.LastIncludedIndex + 1
}

func (log *Log) get(i int) LogEntry {
	if !log.check(i) {
		fmt.Printf("get %v out of log's bound %v - %v\n", i, log.beginIndex(), log.getLastIndex())
		// println(debug.Stack())
		os.Exit(10)
	}
	return log.Entries[i-log.beginIndex()]
}

func (log *Log) getEntryTerm(i int) int {
	if i == log.LastIncludedIndex {
		return log.LastIncludedTerm
	}
	//println("getEntryTerm")
	return log.get(i).Term
}

func (log *Log) getRange(l, r int) []LogEntry {
	if !log.check(l) || !log.check(r-1) {
		fmt.Printf("getRange %v - %v out of log's bound %v - %v\n", l, r, log.beginIndex(), log.getLastIndex())
		os.Exit(11)
	}
	return log.Entries[l-log.beginIndex() : r-log.beginIndex()]
}

func (log *Log) getL(l int) []LogEntry {
	if !(l >= log.beginIndex() && l <= log.getNextIndex()) {
		fmt.Printf("getL %v out of log's bound %v - %v\n", l, log.beginIndex(), log.getNextIndex())
		os.Exit(12)
	}
	return log.Entries[l-log.beginIndex():]
}

func (log *Log) append(entries []LogEntry) {
	for _, entry := range entries {
		log.Entries = append(log.Entries, entry)
	}
}

func (log *Log) appendEntry(entry LogEntry) {
	log.Entries = append(log.Entries, entry)
}

func (log *Log) discardByCnt(cnt int) {
	if cnt > len(log.Entries) {
		fmt.Printf("discard %v entries but there only %v entries left\n", cnt, len(log.Entries))
		os.Exit(13)
	}
	if cnt == 0 {
		return
	}
	log.LastIncludedIndex += cnt
	log.LastIncludedTerm = log.Entries[cnt-1].Term
	log.Entries = log.Entries[cnt:]
}

func (log *Log) discardBeforeAndIncludeIndex(i int) {
	if i == log.LastIncludedIndex {
		fmt.Printf("discard 0 entry for this index %v\n", i)
		return
	}
	if i < log.beginIndex() || i >= log.getNextIndex() {
		fmt.Printf("discard before index %v entries but the legel range should be %v - %v\n", i, log.beginIndex(), log.getLastIndex())
		os.Exit(14)
	}
	// log.printLogInfo()
	tmpLastIncludedIndex, tmpLastIncludedTerm := i, log.getEntryTerm(i)
	log.setL(i + 1)
	log.LastIncludedIndex = tmpLastIncludedIndex
	log.LastIncludedTerm = tmpLastIncludedTerm
	// log.printLogInfo()
	fmt.Printf("discard index before %v(included)\n", i)
}

func (log *Log) getLastIndex() int {
	return log.LastIncludedIndex + len(log.Entries)
}

func (log *Log) setL(l int) { // set log begin with index l
	if l > log.getLastIndex() {
		log.Entries = make([]LogEntry, 0)
		return
	}
	log.Entries = log.Entries[l-log.beginIndex():]
}

func (log *Log) setR(r int) {
	log.Entries = log.Entries[:r-log.beginIndex()]
}

func (log *Log) getNextIndex() int {
	return log.getLastIndex() + 1
}

func (log *Log) printLogInfo() {
	fmt.Printf("LastIncludedIndex: %v\nLastIncludedTerm: %v\ncurrent log:\n", log.LastIncludedIndex, log.LastIncludedTerm)
	for i, entry := range log.Entries {
		fmt.Printf("[index:%v, val:%v, term:%v] ", i+log.beginIndex(), entry.Command, entry.Term)
	}
	fmt.Println()
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
	MinElectionTimeOut          int = 250
	MaxElectionTimeOut          int = 450
	HeartbeatPeriodTime         int = 100
	CommitIndexUpdatePeriodTime int = 10
	applyWaitingTime            int = 5
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
	currentTerm int  // last term server has seen
	votedFor    int  // nil for none, or choose one in a term
	log         *Log // log entries

	// volatile state

	// servers' side
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry known to be applied

	// leaders' side
	nextIndex  []int // index of next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server

	heartBeatFlag bool
	heartTimer    *time.Timer
	// heartBeatCnt int
	// heartBeatMu sync.Mutex

	applyCh *chan ApplyMsg

	cnt          map[int]int
	snapshotData []byte
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshotData)
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
	var voteFor int
	var log *Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("something goes wrong when readPersist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
	}
}

// restore previously persisted state.
func (rf *Raft) readPersistSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	fmt.Printf("server %v read form stored snapshot\n", rf.me)
	rf.snapshotData = data
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 是否需要先判断apply到index了没有（应该不需要）
	rf.mu.Lock()
	if index < rf.log.beginIndex() {
		fmt.Printf("server %v refuses to take snopshot because the snapshot is too old%v\n", rf.me, index)
		rf.mu.Unlock()
		return
	}
	fmt.Printf("server %v takes snopshot and discards logs before index %v\n", rf.me, index)
	rf.log.discardBeforeAndIncludeIndex(index)
	rf.snapshotData = snapshot
	rf.persist()
	fmt.Printf("server %v finished snopshot\n", rf.me)
	rf.mu.Unlock()

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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		// new term then clear voteFor to -1
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
	}
	if rf.currentTerm > args.Term {
		return
	}
	lastLogIndex := rf.log.getLastIndex()
	lastLogTerm := rf.log.getEntryTerm(lastLogIndex)
	if rf.currentTerm > args.Term {
		fmt.Printf("server %v thinks candidate %v term too small\n", rf.me, args.CandidateId)
	} else if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		fmt.Printf("server %v already has a voteFor %v but not candidate %v\n", rf.me, rf.votedFor, args.CandidateId)
	} else if !(args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		fmt.Printf("server %v thinks candidate %v is not up-to-date\n", rf.me, args.CandidateId)
	}
	if rf.currentTerm <= args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		reply.VoteGrant = true
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.heartBeatFlag = true
	}
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *AppendEntryReply) {
	fmt.Printf("server %v receives snopshot from leader %v\n", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
	}
	rf.log.setL(rf.log.getLastIndex() + 1)
	rf.log.LastIncludedIndex = args.LastIncludedIndex
	rf.log.LastIncludedTerm = args.LastIncludeTerm
	rf.lastApplied = rf.log.LastIncludedIndex
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.snapshotData = args.Data
	fmt.Printf("server %v commit snopshot to lastIncludedIndex %v\n", rf.me, args.LastIncludedIndex)

	// 从snapshot中恢复状态机
	*rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.persist()

}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	// when conflicting
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	fmt.Printf("server %v receives appendEntry from leader %v prevLogIndex %v\n", rf.me, args.LeaderId, args.PrevLogIndex)
	rf.mu.Lock()
	fmt.Printf("server %v acquire appendEntry lock succeed\n", rf.me)
	defer fmt.Printf("server %v release appendEntry lock succeed\n", rf.me)
	//defer fmt.Printf("server %v receives appendEntry from leader %v finished\n", rf.me, args.LeaderId)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
	} else if rf.currentTerm == args.Term && rf.state == Candidate {
		rf.convertFollower()
	}
	if rf.currentTerm > args.Term {
		return
	}
	/*if rf.commitIndex < rf.log.LastIncludedIndex {

	}*/
	lastLogIndex := rf.log.getLastIndex()
	if args.Term < rf.currentTerm ||
		(args.PrevLogIndex > rf.commitIndex &&
			(lastLogIndex < args.PrevLogIndex || rf.log.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm)) {
		if lastLogIndex < args.PrevLogIndex || rf.log.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
			fmt.Printf("lastLogIndex %v, args.PrevLogIndex %v\n", lastLogIndex, args.PrevLogIndex)
			if lastLogIndex >= args.PrevLogIndex {
				fmt.Printf("rf.log.getEntryTerm(args.PrevLogIndex) %v, args.PrevLogTerm %v\n", rf.log.getEntryTerm(args.PrevLogIndex), args.PrevLogTerm)
			}
		}
		// when conflicting
		reply.XLen = rf.log.getNextIndex()
		if args.Term >= rf.currentTerm && lastLogIndex >= args.PrevLogIndex {
			reply.XTerm = rf.log.getEntryTerm(args.PrevLogIndex)
			lo, hi := rf.log.beginIndex(), args.PrevLogIndex
			for lo <= hi {
				mid := (lo + hi) / 2
				if rf.log.getEntryTerm(mid) >= reply.XTerm {
					hi = mid - 1
				} else {
					lo = mid + 1
				}
			}
			reply.XIndex = lo
			if reply.XIndex == 0 {
				fmt.Printf("wrong lo: \n%v\n%v\n%v\n", rf.log.getEntryTerm(args.PrevLogIndex), args.PrevLogIndex, args.PrevLogTerm)
				os.Exit(15)
			}
		}
		if args.Term >= rf.currentTerm && lastLogIndex >= args.PrevLogIndex && args.PrevLogIndex > rf.commitIndex {
			rf.log.setR(args.PrevLogIndex)
		}
		reply.Success = false
	} else {
		// fmt.Printf("current commitIndex %v lastIncludedIndex %v snapshot exists ? %v\n", rf.commitIndex, rf.log.LastIncludedIndex, rf.snapshotData != nil)
		if rf.commitIndex < rf.log.LastIncludedIndex {
			rf.log.printLogInfo()
		}
		rf.log.setR(max(args.PrevLogIndex+1, rf.commitIndex+1))
		for i, entry := range args.Entries {
			ptr := i + args.PrevLogIndex + 1
			if ptr > rf.commitIndex {
				rf.log.appendEntry(entry)
			}
		}
		rf.persist()
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, rf.log.getLastIndex())
			// 用select改一下，避免applyCH太满了
			/*nxtCommitIndex := newCommitIndex
			flag := false
			for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				select {
				case *rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log.get(i).Command,
					CommandIndex: i,
				}:
					fmt.Printf("follower server %v commit index %v command %v \n", rf.me, i, rf.log.get(i).Command)
					continue
				default:
					nxtCommitIndex = i - 1
					flag = true
					fmt.Printf("server %v applyCh is full \n", rf.me)
					break

				}
				if flag {
					break
				}
			}
			rf.commitIndex = nxtCommitIndex*/
			//------------
			/*for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				fmt.Printf("leader server %v commit index %v command %v\n", rf.me, i, rf.log.get(i).Command)
				*rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log.get(i).Command,
					CommandIndex: i,
				}
			}*/
			rf.commitIndex = newCommitIndex
		}
		reply.Success = true
	}
	rf.heartBeatFlag = true

	// go rf.heartBeatReceived()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.mu.Lock()
	isLeader = rf.state == Leader
	term = rf.currentTerm
	if isLeader {
		fmt.Printf("server %v append command %v\n", rf.me, command)
		// append
		index = rf.log.getNextIndex()
		rf.log.appendEntry(LogEntry{Term: term, Command: command})
		rf.persist()
		fmt.Printf("len(log) = %v\n", rf.log.getLastIndex())
		rf.ResetHeartTimer(5)
	}
	rf.mu.Unlock()

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
		term, isleader := rf.GetState()
		fmt.Printf("server %v term %v state %v(leader %v)(heartFlag %v)\n", rf.me, term, rf.state, isleader, rf.heartBeatFlag)
		if rf.getState() == Leader {
			rf.printAllInfo()
		}
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

func (rf *Raft) printAllInfo() {
	println("nextIndex")
	for _, v := range rf.nextIndex {
		fmt.Printf("%v || ", v)
	}
	fmt.Println()
	println("matchIndex")
	for _, v := range rf.matchIndex {
		fmt.Printf("%v || ", v)
	}
	fmt.Println()
}

func (rf *Raft) check() bool {

	rf.mu.Lock()
	flag := !rf.heartBeatFlag && rf.state == Follower
	rf.mu.Unlock()
	return flag
}

func (rf *Raft) startElection() {
	fmt.Printf("server %v term %v doesn't hear any heartbeat\n", rf.me, rf.currentTerm)
	for {
		rf.mu.Lock()
		// send RequestVote RPCs to all other servers
		rf.convertCandidate()
		rf.cnt[rf.currentTerm] = 1
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			request := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log.getLastIndex(),
				LastLogTerm:  rf.log.getEntryTerm(rf.log.getLastIndex()),
			}
			rf.mu.Unlock()
			go func(i int, request *RequestVoteArgs) {
				reply := RequestVoteReply{}
				if rf.getState() != Candidate || request.Term != rf.currentTerm {
					return
				}
				ok := rf.sendRequestVote(i, request, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.convertFollower()
						rf.persist()
						return
						// 退出选举
					}
					if reply.VoteGrant && request.Term == rf.currentTerm {
						rf.cnt[request.Term]++
						if rf.state == Candidate && rf.cnt[rf.currentTerm] > len(rf.peers)/2 {
							rf.convertLeader()
							return
						}
					}
				}
			}(i, &request)
		}
		time.Sleep(getDuration(getBoundRand(MinElectionTimeOut, MaxElectionTimeOut)))
		if rf.getState() != Candidate {
			break
		}
		fmt.Printf("server %v term %v ElectionTimeOut\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) convertFollower() {
	fmt.Printf("server %v convert to follower\n", rf.me)
	rf.state = Follower
}

func (rf *Raft) convertCandidate() {
	fmt.Printf("server %v convert to candidate\n", rf.me)
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) convertLeader() {
	fmt.Printf("server %v convert to leader\n", rf.me)
	rf.state = Leader
	rf.leaderInit()
	go rf.leaderSendAppendEntries()
	go rf.updateLeaderCommitIndex()
}

func (rf *Raft) getState() int {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	return state

}

func (rf *Raft) leaderInit() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.getNextIndex()
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.log.getLastIndex()
}

func (rf *Raft) leaderSendAppendEntries() {
	// fmt.Printf("server %v term %v send appendEntries\n", rf.me, rf.currentTerm)

	for rf.getState() == Leader && !rf.killed() {
		// fmt.Printf("server %v term %v send appendEntries\n", rf.me, rf.currentTerm)
		// rf.printAllInfo()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			fmt.Printf("server %v term %v send appendEntries to server %v\n", rf.me, rf.currentTerm, i)
			if rf.nextIndex[i] <= rf.log.LastIncludedIndex {
				// send snopshot instead
				/*if rf.getState() != Leader {
					return
				}*/
				term := rf.currentTerm
				id := rf.me
				lastIncludedIndex := rf.log.LastIncludedIndex
				lastIncludedTerm := rf.log.LastIncludedTerm
				data := rf.snapshotData
				// sendInstallSnapshot
				/*if rf.getState() != Leader {
					return
				}*/
				rf.mu.Unlock()
				go func(i, term, id, lastIncludedIndex, lastIncludedTerm int, data []byte) {
					args := InstallSnapshotArgs{
						Term:              term,
						LeaderId:          id,
						LastIncludedIndex: lastIncludedIndex,
						LastIncludeTerm:   lastIncludedTerm,
						Data:              data,
					}
					reply := InstallSnapshotReply{}
					if currentTerm, isLeader := rf.GetState(); isLeader && args.Term == currentTerm {
						ok := rf.sendInstallSnapshot(i, &args, &reply)
						if ok {
							rf.updateLeaderSnapshotVersion(&reply, &args, i)
						}
					}
				}(i, term, id, lastIncludedIndex, lastIncludedTerm, data)
				continue
			}
			// rf.printAllInfo()
			fmt.Printf("server %v, nextIndex %v \n", i, rf.nextIndex[i])
			entries := rf.log.getL(rf.nextIndex[i])
			term := rf.currentTerm
			id := rf.me
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.log.getEntryTerm(prevLogIndex)
			leaderCommit := rf.commitIndex
			rf.mu.Unlock()

			// fmt.Printf("server %v term %v send appendEntries to %v outer \n", rf.me, rf.currentTerm, i)
			go func(i, term, id, prevLogIndex, prevLogTerm, leaderCommit int, entries []LogEntry) {
				// fmt.Printf("server %v term %v send appendEntries to %v iner \n", rf.me, rf.currentTerm, i)
				args := AppendEntryArgs{
					Term:         term,
					LeaderId:     id,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				len := len(args.Entries)
				reply := AppendEntryReply{}
				if currentTerm, isLeader := rf.GetState(); isLeader && args.Term == currentTerm {
					ok := rf.sendAppendEntries(i, &args, &reply)
					if ok {
						rf.updateLeader(&reply, i, len, &args)
					}
				}
			}(i, term, id, prevLogIndex, prevLogTerm, leaderCommit, entries)
		}
		// update leader's commitIndex
		// search for matchIndex, and find middle one
		// time.Sleep(getDuration(HeartbeatPeriodTime))
		rf.ResetHeartTimer(HeartbeatPeriodTime)
		<-rf.heartTimer.C
	}
}

func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

func (rf *Raft) updateLeaderCommitIndex() {
	for rf.getState() == Leader {
		rf.mu.Lock()
		rf.nextIndex[rf.me] = rf.log.getNextIndex()
		rf.matchIndex[rf.me] = rf.log.getLastIndex()
		//rf.mu.Unlock()
		//go func() {
		//rf.mu.Lock()
		tmp := make([]int, len(rf.peers))
		for i, v := range rf.matchIndex {
			tmp[i] = v
		}
		sort.Ints(tmp)
		newCommitIndex := tmp[(len(rf.peers)-1)/2]
		if newCommitIndex > rf.commitIndex &&
			rf.log.getEntryTerm(newCommitIndex) == rf.currentTerm {
			// -------------------
			/*nxtCommitIndex := newCommitIndex
			flag := false
			for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				select {
				case *rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log.get(i).Command,
					CommandIndex: i,
				}:
					fmt.Printf("follower server %v commit index %v command %v \n", rf.me, i, rf.log.get(i).Command)
					continue
				default:
					nxtCommitIndex = i - 1
					fmt.Printf("server %v applyCh is full \n", rf.me)
					flag = true
					break
				}
				if flag {
					break
				}
			}
			rf.commitIndex = nxtCommitIndex*/

			/*for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				fmt.Printf("leader server %v commit index %v command %v\n", rf.me, i, rf.log.get(i).Command)
				*rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log.get(i).Command,
					CommandIndex: i,
				}
			}*/
			rf.commitIndex = newCommitIndex
		}
		rf.mu.Unlock()
		//}()
		time.Sleep(getDuration(CommitIndexUpdatePeriodTime))
	}

}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) updateLeaderSnapshotVersion(reply *InstallSnapshotReply, args *InstallSnapshotArgs, ptr int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	rf.nextIndex[ptr] = args.LastIncludedIndex + 1
	rf.matchIndex[ptr] = rf.nextIndex[ptr] - 1
}

func (rf *Raft) updateLeader(reply *AppendEntryReply, ptr int, lenEntries int, args *AppendEntryArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Success {
		rf.nextIndex[ptr] = args.PrevLogIndex + 1 + lenEntries
		fmt.Printf("update server %v 's matchIndex from %v to %v\n", ptr, rf.matchIndex[ptr], rf.nextIndex[ptr]-1)
		rf.matchIndex[ptr] = rf.nextIndex[ptr] - 1
		if rf.nextIndex[ptr] > rf.log.getNextIndex() {
			fmt.Printf("args info:\nargs.Term:%v\npreIndex:%v\npreTerm:%v\nlen:%v\n", args.Term, args.PrevLogIndex, args.PrevLogTerm, lenEntries)
			fmt.Printf("leader %v state %v term %v wrong nextIndex out of bound. other info:\n", rf.me, rf.state == Leader, rf.currentTerm)
			os.Exit(16)
		}
	} else {
		// optimization: bypass all the conflicting entries in that term
		/*lstTerm := rf.log[rf.nextIndex[ptr]-1].Term
		lo, hi := 0, rf.nextIndex[ptr]-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if rf.log[mid].Term >= lstTerm {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		// rf.nextIndex[ptr]--
		rf.nextIndex[ptr] = lo*/
		if reply.XLen < args.PrevLogIndex+1 {
			// follower's log is too short
			rf.nextIndex[ptr] = min(reply.XLen, rf.nextIndex[ptr])
			if rf.nextIndex[ptr] == 0 {
				fmt.Printf("wrong nextIndex 0. other info:\n%v\n", reply.XLen)
				os.Exit(17)
			}
			return
		}
		// find whether leader has XTerm
		lo, hi := rf.log.beginIndex(), rf.nextIndex[ptr]-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if rf.log.getEntryTerm(mid) > reply.XTerm {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		if rf.log.getEntryTerm(hi) == reply.XTerm {
			// leader has XTerm
			rf.nextIndex[ptr] = min(lo, rf.nextIndex[ptr])
		} else {
			rf.nextIndex[ptr] = min(reply.XIndex, rf.nextIndex[ptr])
		}
		if rf.nextIndex[ptr] == 0 {
			fmt.Printf("args info:\nargs.Term:%v\npreIndex:%v\npreTerm:%v\n", args.Term, args.PrevLogIndex, args.PrevLogTerm)
			fmt.Printf("leader %v state %v term %v wrong nextIndex 0. other info:\nXTerm equal? %v\nif equal nextIndex[%v]:%v\nelse:%v\nreply.XTerm:%v\nXlen:%v\nreplay.Term:%v\n", rf.me, rf.state == Leader, rf.currentTerm, rf.log.getEntryTerm(hi) == reply.XTerm, ptr, lo, reply.XIndex, reply.XTerm, reply.XLen, reply.Term)
			os.Exit(18)
		}
	}
}

func getBoundRand(l, r int) int {
	return l + rand.Int()%(r-l+1)
}

func getDuration(t int) time.Duration {
	return time.Duration(t) * time.Millisecond
}

func (rf *Raft) getSnapShot() []byte {
	return rf.snapshotData
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
		log:           &Log{Entries: make([]LogEntry, 0), LastIncludedIndex: 0, LastIncludedTerm: -1}, // first index is 1
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, cnt),
		matchIndex:    make([]int, cnt),
		heartBeatFlag: false,
		applyCh:       &applyCh,
		cnt:           make(map[int]int),
		snapshotData:  nil,
		heartTimer:    time.NewTimer(getDuration(0)),
		// heartBeatCnt: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readPersistSnapshot(persister.ReadSnapshot())
	if rf.snapshotData != nil {
		fmt.Printf("server %v recover from snopshot\n", rf.me)
		/**rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshotData,
			SnapshotTerm:  rf.log.LastIncludedTerm,
			SnapshotIndex: rf.log.LastIncludedIndex,
		}*/
		rf.commitIndex = rf.log.LastIncludedIndex
		rf.lastApplied = rf.log.LastIncludedIndex
		fmt.Printf("server %v finishes recovery\n", rf.me)
	} else {
		fmt.Printf("server %v no snopshot yet\n", rf.me)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go func() {
		for {
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			// lstApplied := rf.lastApplied
			rf.mu.Unlock()
			for rf.getLastApplied() < commitIndex {
				rf.mu.Lock()
				index := rf.lastApplied + 1
				if index > commitIndex {
					rf.mu.Unlock()
					break
				}
				command := rf.log.get(index).Command
				rf.mu.Unlock()
				fmt.Printf("server %v commit index %v command %v\n", rf.me, index, command)
				*rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: index,
				}
				rf.mu.Lock()
				rf.lastApplied++
				rf.mu.Unlock()
			}
			time.Sleep(getDuration(applyWaitingTime))

		}
	}()

	return rf
}

func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	lstApplied := rf.lastApplied
	rf.mu.Unlock()
	return lstApplied
}
