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
	ApplyPeriodTime    = 25
	WaitingAppliedTime = 15
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
	Type  string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage    map[string]string
	applyIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(&Op{
		Type: "Get",
		Key:  args.Key,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	fmt.Printf("op:Get key:%v\n", args.Key)
	kv.mu.Unlock()

	for {
		kv.mu.Lock()
		if curTerm, _ := kv.rf.GetState(); curTerm != term {
			reply.Err = ErrTermChanged
			return
		}
		if kv.applyIndex >= index {
			break
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(WaitingAppliedTime) * time.Millisecond)
	}
	/*<-kv.applyCh
	val, ok := kv.storage[args.Key]
	reply.Value = val
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}*/

}

func (kv *KVServer) GetAppliedIndex() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.applyIndex
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//defer kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(&Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("op:%v key:%v val:%v\n", args.Op, args.Key, args.Value)
	kv.mu.Unlock()

	for {
		kv.mu.Lock()
		if curTerm, _ := kv.rf.GetState(); curTerm != term {
			reply.Err = ErrTermChanged
			return
		}
		if kv.applyIndex >= index {
			break
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(WaitingAppliedTime) * time.Millisecond)
	}
	//<-kv.applyCh
	reply.Err = OK
	/*if args.Op == "Put" {
		kv.storage[args.Key] = args.Value
	} else if args.Op == "Append" {
		kv.storage[args.Key] = kv.storage[args.Key] + args.Value
	}*/
}

func (kv *KVServer) ApplyCommand(op *Op) {
	switch op.Type {
	case "Get":
		return
	case "Put":
		kv.storage[op.Key] = op.Value
		return
	case "Append":
		kv.storage[op.Key] = kv.storage[op.Key] + op.Value
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

	applyCh := make(chan raft.ApplyMsg, 10)
	kv := KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),
		storage:      make(map[string]string),
		applyIndex:   0,
	}
	// You may need initialization code here.

	go func() {
		for {
			kv.mu.Lock()
			chanIsEmpty := false
			for !chanIsEmpty {
				select {
				case applyMsg := <-applyCh:
					op := applyMsg.Command.(Op)
					kv.ApplyCommand(&op)
					kv.applyIndex = applyMsg.CommandIndex
				default:
					chanIsEmpty = true
					// fmt.Printf("")

				}
			}
			kv.mu.Unlock()
			time.Sleep(time.Duration(ApplyPeriodTime) * time.Millisecond)

		}
	}()

	return &kv
}
