package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	TryNextNodeTime = 5
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clerkId  int64
	seq      int
	// cntServers int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	fmt.Printf("creat client")
	ck := Clerk{
		servers:  servers,
		leaderId: 0,
		// cntServers: len(servers),
		seq:     0,
		clerkId: nrand(),
	}
	// You'll have to add code here.
	return &ck
}

func (ck *Clerk) GetSeq() int {
	ck.seq++
	return ck.seq
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//fmt.Printf("Get key:%v\n", key)
	args := GetArgs{
		Key:     key,
		Seq:     ck.GetSeq(),
		ClerkId: ck.clerkId,
	}

	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case ErrWrongLeader, ErrTermChanged:
				fmt.Printf("Leader %v changed, tryNewLeader: %v \n", ck.leaderId, (ck.leaderId+1)%len(ck.servers))
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				time.Sleep(TryNextNodeTime * time.Millisecond)
				continue
			case ErrTimeOut:
				fmt.Printf("err time out\n")
				continue
			case OK:
				fmt.Printf("Get key:%v succeed (leaderid:%v)\n", key, ck.leaderId)
				return reply.Value
			}
		} else {
			fmt.Printf("cannot reach server %v, try server: %v \n", ck.leaderId, (ck.leaderId+1)%len(ck.servers))
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(TryNextNodeTime * time.Millisecond)
			continue
		}

	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//fmt.Printf("%v key:%v val:%v\n", op, key, value)
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		Seq:     ck.GetSeq(),
		ClerkId: ck.clerkId,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case ErrWrongLeader, ErrTermChanged:
				fmt.Printf("Leader %v changed, tryNewLeader: %v \n", ck.leaderId, (ck.leaderId+1)%len(ck.servers))
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				time.Sleep(TryNextNodeTime * time.Millisecond)
				continue
			case ErrTimeOut:
				fmt.Printf("err time out\n")
				continue
			case OK:
				fmt.Printf("%v key:%v val:%v succeed (leaderid:%v)\n", op, key, value, ck.leaderId)
				return
			}
		} else {
			fmt.Printf("cannot reach server %v, try server: %v \n", ck.leaderId, (ck.leaderId+1)%len(ck.servers))
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(TryNextNodeTime * time.Millisecond)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	// fmt.Printf("Put key val:%v, %v", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	// fmt.Printf("Append key val:%v, %v", key, value)
	ck.PutAppend(key, value, "Append")
}
