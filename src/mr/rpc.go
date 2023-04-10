package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type MapArgs struct {
	ID int
}

type MapReply struct {
	Files   []string
	NReduce int
	MapID   int
	Ok      bool
}

type MapEndArgs struct {
	ID    int
	MapID int
}

type MapEndReply struct {
	Success bool
}

type TaskArgs struct {
	ID int
}

type TaskReply struct {
	MapTask    bool
	ReduceTask bool
}

type ReduceArgs struct {
	ID int
}

type ReduceReply struct {
	ReduceID int
	Cnt      int
	Ok       bool
}

type ReduceEndArgs struct {
	ID       int
	ReduceID int
}

type ReduceEndReply struct {
	Success bool
}

type InitArgs struct {
}

type InitReply struct {
	ID int
}
