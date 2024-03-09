package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
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
	storage          map[string]string
	applyIndex       int
	clientSeqApplied map[int64]int
	replyChannel     map[int64]chan Res
}

type Res struct {
	index int
	term  int
	val   string
	seq   int
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
	for {
		select {
		case applyRes := <-kv.replyChannel[args.ClerkId]:
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
	for {
		select {
		case applyRes := <-kv.replyChannel[args.ClerkId]:
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

func (kv *KVServer) EmitApplyChannel() {
	// 有apply信息立马消费
	for {
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)
		kv.mu.Lock()
		kv.ApplyCommand(&op, applyMsg.CommandIndex)
		kv.applyIndex = applyMsg.CommandIndex
		kv.mu.Unlock()
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
		me:               me,
		maxraftstate:     maxraftstate,
		applyCh:          applyCh,
		rf:               raft.Make(servers, me, persister, applyCh),
		storage:          make(map[string]string),
		applyIndex:       -1,
		clientSeqApplied: make(map[int64]int),
		replyChannel:     make(map[int64]chan Res),
	}
	// You may need initialization code here.

	go kv.EmitApplyChannel()

	return &kv
}
