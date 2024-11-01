package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"raft_LSMTree-based_KVStore/labgob"
	"raft_LSMTree-based_KVStore/labrpc"
	"raft_LSMTree-based_KVStore/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const (
	ApplyPeriodTime       = 5
	WaitingAppliedTime    = 10
	CheckTimeOut          = 2000
	putIntoChannelTimeOut = 10
	threshold             = 0.8
	SnapshotCheckTime     = 20
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
	ClerkID int64
	Seq     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastIncludedIndex int
	// lastIncludedTerm  int
	storage          map[string]string
	clientSeqApplied map[int64]int
	replyChannel     map[int64]chan Res
}

type Res struct {
	index int
	term  int
	val   string
	seq   int
}

func (kv *KVServer) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.storage)
	e.Encode(kv.clientSeqApplied)
	e.Encode(kv.lastIncludedIndex)
	machineState := w.Bytes()
	return machineState
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var clientSeqApplied map[int64]int
	var lastIncludedIndex int
	if d.Decode(&storage) != nil ||
		d.Decode(&clientSeqApplied) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		fmt.Printf("something goes wrong when readKVServerPersist\n")
	} else {
		kv.storage = storage
		kv.clientSeqApplied = clientSeqApplied
		kv.lastIncludedIndex = lastIncludedIndex
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// start 不需要加锁，无kv数据、raft本身有锁（且内有持久化）
	_, term, isLeader := kv.rf.Start(Op{
		Type:    "Get",
		Key:     args.Key,
		ClerkID: args.ClerkId,
		Seq:     args.Seq,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 接收到对应的 apply 信息（在一定时间内）
	// 只有 Get 可能会返回
	kv.mu.Lock()
	_, exist := kv.replyChannel[args.ClerkId]
	if !exist {
		kv.replyChannel[args.ClerkId] = make(chan Res, 10)
	}
	input := kv.replyChannel[args.ClerkId]
	kv.mu.Unlock()
	for {
		select {
		case applyRes := <-input:
			if applyRes.seq < args.Seq {
				println("get seq too small")
				continue
			} else {
				fmt.Printf("get succeed key:%v \n", args.Key)
				reply.Value = applyRes.val
				reply.Err = OK
				if curTerm, _ := kv.rf.GetState(); curTerm != term {
					reply.Err = ErrTermChanged
				}
				return
			}
		case <-time.After(CheckTimeOut * time.Millisecond):
			println("get timeout")
			reply.Err = ErrTimeOut
			return
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, term, isLeader := kv.rf.Start(Op{
		Type:    args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkID: args.ClerkId,
		Seq:     args.Seq,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("op:%v key:%v val:%v\n", args.Op, args.Key, args.Value)
	if curTerm, _ := kv.rf.GetState(); curTerm != term {
		reply.Err = ErrTermChanged
		return
	}
	kv.mu.Lock()
	_, exist := kv.replyChannel[args.ClerkId]
	if !exist {
		kv.replyChannel[args.ClerkId] = make(chan Res, 10)
	}
	input := kv.replyChannel[args.ClerkId]
	kv.mu.Unlock()
	for {
		select {
		case applyRes := <-input:
			if applyRes.seq < args.Seq {
				println("putappend seq too small")
				continue
			} else {
				fmt.Printf("putappend succeed key:%v val:%v\n", args.Key, args.Value)
				reply.Err = OK
				if curTerm, _ := kv.rf.GetState(); curTerm != term {
					reply.Err = ErrTermChanged
				}
				return
			}
		case <-time.After(CheckTimeOut * time.Millisecond):
			println("putappend timeout")
			reply.Err = ErrTimeOut
			return
		}
	}
}

func mx(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (kv *KVServer) EmitApplyChannel() {
	// 有apply信息立马消费
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			kv.ApplyCommand(&op, applyMsg.CommandIndex)
			kv.lastIncludedIndex = mx(applyMsg.CommandIndex, kv.lastIncludedIndex)
			//kv.lastIncludedTerm = applyMsg.
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			kv.lastIncludedIndex = mx(applyMsg.SnapshotIndex, kv.lastIncludedIndex)
			// kv.lastIncludedTerm = applyMsg.SnapshotTerm
			// kv.rf.Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
			kv.readPersist(applyMsg.Snapshot)
			kv.mu.Unlock()
		}

	}
}

func (kv *KVServer) checkNotDuplicate(op *Op) bool {
	return kv.clientSeqApplied[op.ClerkID] < op.Seq
}

func (kv *KVServer) ApplyCommand(op *Op, index int) {
	fmt.Printf("apply op:%v key:%v val:%v(clerkid:%v)\n", op.Type, op.Key, op.Value, op.ClerkID)
	switch op.Type {
	case "Get":
		// leader发送命令给对应的请求来处理
		if _, isLeader := kv.rf.GetState(); isLeader {
			_, exist := kv.replyChannel[op.ClerkID]
			if !exist {
				kv.replyChannel[op.ClerkID] = make(chan Res, 10)
			}
			select {
			case kv.replyChannel[op.ClerkID] <- Res{index: index, val: kv.storage[op.Key], seq: op.Seq}:
				return
			case <-time.After(putIntoChannelTimeOut * time.Millisecond):
				/*for {
					select {
					case <-kv.replyChannel[op.ClerkID]:
						continue
					default:
						return
					}
				}*/
				return
			}
		}
		return
	case "Put":
		if kv.checkNotDuplicate(op) {
			kv.clientSeqApplied[op.ClerkID] = op.Seq
			kv.storage[op.Key] = op.Value
		}

		if _, isLeader := kv.rf.GetState(); isLeader {
			_, exist := kv.replyChannel[op.ClerkID]
			if !exist {
				kv.replyChannel[op.ClerkID] = make(chan Res, 10)
			}
			select {
			case kv.replyChannel[op.ClerkID] <- Res{index: index, seq: op.Seq}:
				return
			case <-time.After(putIntoChannelTimeOut * time.Millisecond):
				return
			}
		}
		return
	case "Append":
		if kv.checkNotDuplicate(op) {
			kv.clientSeqApplied[op.ClerkID] = op.Seq
			kv.storage[op.Key] = kv.storage[op.Key] + op.Value
		}
		if _, isLeader := kv.rf.GetState(); isLeader {
			_, exist := kv.replyChannel[op.ClerkID]
			if !exist {
				kv.replyChannel[op.ClerkID] = make(chan Res, 10)
			}
			select {
			case kv.replyChannel[op.ClerkID] <- Res{index: index, seq: op.Seq}:
				return
			case <-time.After(putIntoChannelTimeOut * time.Millisecond):
				return
			}
		}
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg, 10000)
	kv := KVServer{
		me:                me,
		maxraftstate:      maxraftstate,
		applyCh:           applyCh,
		rf:                raft.Make(servers, me, persister, applyCh),
		storage:           make(map[string]string),
		lastIncludedIndex: 0,
		clientSeqApplied:  make(map[int64]int),
		replyChannel:      make(map[int64]chan Res),
	}
	// You may need initialization code here.
	kv.readPersist(persister.ReadSnapshot())

	go kv.EmitApplyChannel()
	go func() {
		for {
			if kv.maxraftstate != -1 {

				kv.mu.Lock()
				if kv.maxraftstate*4/5 <= persister.RaftStateSize() {
					fmt.Printf("raft log too huge, do snapshot\n")
					kv.rf.Snapshot(kv.lastIncludedIndex, kv.generateSnapshot())
				}
				kv.mu.Unlock()
			}
			time.Sleep(SnapshotCheckTime * time.Millisecond)
		}
	}()

	return &kv
}
