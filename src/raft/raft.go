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
	"6.5840/labgob"
	"bytes"
	"fmt"
	"os"
	"sort"

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
	MinElectionTimeOut          int = 200
	MaxElectionTimeOut          int = 450
	HeartbeatPeriodTime         int = 50
	CommitIndexUpdatePeriodTime int = 10
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
	// heartBeatMu sync.Mutex

	applyCh *chan ApplyMsg

	cnt map[int]int
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
	rf.persister.Save(raftstate, nil)
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
	var log []LogEntry
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
		// new term then clear voteFor to -1
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
	}
	lastLogIndex := rf.getLastLogIndex()
	if rf.currentTerm > args.Term {
		fmt.Printf("server %v thinks candidate %v term too small\n", rf.me, args.CandidateId)
	} else if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		fmt.Printf("server %v already has a voteFor %v but not candidate %v\n", rf.me, rf.votedFor, args.CandidateId)
	} else if !(args.LastLogTerm > rf.log[lastLogIndex].Term || args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= rf.getLastLogIndex()) {
		fmt.Printf("server %v thinks candidate %v is not up-to-date\n", rf.me, args.CandidateId)
	}
	if rf.currentTerm <= args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[lastLogIndex].Term || args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= rf.getLastLogIndex()) {
		reply.VoteGrant = true
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.heartBeatFlag = true
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

	// when conflicting
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	fmt.Printf("server %v receives appendEntry from leader %v\n", rf.me, args.LeaderId)
	rf.mu.Lock()
	rf.heartBeatFlag = true
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.convertFollower()
		rf.persist()
	} else if rf.currentTerm == args.Term && rf.state == Candidate {
		rf.convertFollower()
	}
	if args.Term < rf.currentTerm ||
		(rf.getLastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		if rf.getLastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			fmt.Printf("rf.getLastLogIndex() %v, args.PrevLogIndex %v\n", rf.getLastLogIndex(), args.PrevLogIndex)
			if rf.getLastLogIndex() >= args.PrevLogIndex {
				fmt.Printf("rf.log[args.PrevLogIndex].Term %v, args.PrevLogTerm %v\n", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			}
		}
		// when conflicting
		reply.XLen = len(rf.log)
		if args.Term >= rf.currentTerm && rf.getLastLogIndex() >= args.PrevLogIndex {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			lo, hi := 0, args.PrevLogIndex
			for lo <= hi {
				mid := (lo + hi) / 2
				if rf.log[mid].Term >= reply.XTerm {
					hi = mid - 1
				} else {
					lo = mid + 1
				}
			}
			reply.XIndex = lo
			if reply.XIndex == 0 {
				fmt.Printf("wrong lo: \n%v\n%v\n%v\n", rf.log[args.PrevLogIndex].Term, args.PrevLogIndex, args.PrevLogTerm)
				os.Exit(1002)
			}
		}
		if args.Term >= rf.currentTerm && rf.getLastLogIndex() >= args.PrevLogIndex {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		reply.Success = false
	} else {
		rf.log = rf.log[:args.PrevLogIndex+1]
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		if len(args.Entries) != 0 {
			rf.persist()
		}
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, rf.getLastLogIndex())
			go func(lstIndex, newCommitIndex int) {
				for i := lstIndex + 1; i <= newCommitIndex; i++ {
					fmt.Printf("follower server %v commit index %v command %v \n", rf.me, i, rf.log[i].Command)
					*rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
				}
			}(rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
		}
		reply.Success = true
	}
	rf.heartBeatFlag = true
	rf.mu.Unlock()
	// go rf.heartBeatReceived()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
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

// deprecated
func (rf *Raft) heartBeatReceived() {
	// rf.heartBeatMu.Lock()
	// rf.heartBeatCnt++
	//rf.heartBeatMu.Unlock()
	time.Sleep(getDuration(getBoundRand(MinElectionTimeOut, MaxElectionTimeOut)))
	// rf.heartBeatMu.Lock()
	// rf.heartBeatCnt--
	// rf.heartBeatMu.Unlock()
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
		fmt.Printf("append command %v\n", command)
		// append
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.persist()
		fmt.Printf("len(log) = %v\n", len(rf.log))
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
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.log[rf.getLastLogIndex()].Term,
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
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
}

func (rf *Raft) leaderSendAppendEntries() {
	fmt.Printf("server %v term %v send appendEntries\n", rf.me, rf.currentTerm)

	for rf.getState() == Leader && !rf.killed() {

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			entries := rf.log[rf.nextIndex[i]:]
			term := rf.currentTerm
			id := rf.me
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.log[rf.nextIndex[i]-1].Term
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
		time.Sleep(getDuration(HeartbeatPeriodTime))
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	for rf.getState() == Leader {
		rf.mu.Lock()
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
		rf.mu.Unlock()
		go func() {
			rf.mu.Lock()
			tmp := make([]int, len(rf.matchIndex))
			for i, v := range rf.matchIndex {
				tmp[i] = v
			}
			sort.Ints(tmp)
			newCommitIndex := tmp[len(rf.matchIndex)/2]
			if newCommitIndex > rf.commitIndex &&
				rf.log[newCommitIndex].Term == rf.currentTerm {
				for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
					fmt.Printf("leader server %v commit index %v command %v\n", rf.me, i, rf.log[i].Command)
					*rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,

						// For 2D:
						// SnapshotValid bool
						// Snapshot      []byte
						// SnapshotTerm  int
						// SnapshotIndex int

					}
				}
				rf.commitIndex = newCommitIndex
			}
			rf.mu.Unlock()
		}()
		time.Sleep(getDuration(CommitIndexUpdatePeriodTime))
	}

}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
		if rf.nextIndex[ptr] > len(rf.log) {
			fmt.Printf("args info:\nargs.Term:%v\npreIndex:%v\npreTerm:%v\nlen:%v\n", args.Term, args.PrevLogIndex, args.PrevLogTerm, lenEntries)
			fmt.Printf("leader %v state %v term %v wrong nextIndex out of bound. other info:\n", rf.me, rf.state == Leader, rf.currentTerm)
			os.Exit(10086)
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
				os.Exit(10086)
			}
			return
		}
		// find whether leader has XTerm
		lo, hi := 0, rf.nextIndex[ptr]-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if rf.log[mid].Term > reply.XTerm {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		if rf.log[hi].Term == reply.XTerm {
			// leader has XTerm
			rf.nextIndex[ptr] = min(lo, rf.nextIndex[ptr])
		} else {
			rf.nextIndex[ptr] = min(reply.XIndex, rf.nextIndex[ptr])
		}
		if rf.nextIndex[ptr] == 0 {
			fmt.Printf("args info:\nargs.Term:%v\npreIndex:%v\npreTerm:%v\n", args.Term, args.PrevLogIndex, args.PrevLogTerm)
			fmt.Printf("leader %v state %v term %v wrong nextIndex 0. other info:\nXTerm equal? %v\nif equal nextIndex[%v]:%v\nelse:%v\nreply.XTerm:%v\nXlen:%v\nreplay.Term:%v\n", rf.me, rf.state == Leader, rf.currentTerm, rf.log[hi].Term == reply.XTerm, ptr, lo, reply.XIndex, reply.XTerm, reply.XLen, reply.Term)
			os.Exit(10086)
		}
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
		log:           make([]LogEntry, 0), // first index is 1
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, cnt),
		matchIndex:    make([]int, cnt),
		heartBeatFlag: false,
		applyCh:       &applyCh,
		cnt:           make(map[int]int),
		// heartBeatCnt: 0,
	}
	rf.log = append(rf.log, LogEntry{Term: -1})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
