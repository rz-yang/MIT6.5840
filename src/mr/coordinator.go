package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files            []string
	unstartedFilesID []int
	nReduce          int
	unstartedNum     int
	inProgressNum    int
	finishedNum      int
	finished         map[int]bool
	mutex            sync.Mutex
	finishReduce     int
	inPorgressReduce int
	unstartedReduce  []int
	finishedReduce   map[int]bool
	wg               sync.WaitGroup
	noReplyWorker    map[int]bool
	nxtWorkerID      int
	backupReduce     []int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}
func (c *Coordinator) InitWorker(args *InitArgs, reply *InitReply) error {
	fmt.Printf("get init query\n")
	c.mutex.Lock()
	reply.ID = c.nxtWorkerID
	c.nxtWorkerID++
	c.mutex.Unlock()
	fmt.Printf("init query finished\n")
	return nil
}

func (c *Coordinator) TaskAlready(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	if c.noReplyWorker[args.ID] {
		fmt.Printf("the worker %v may be crashed or no end signal received yet, thus no task assigned to it\n", args.ID)
		reply.MapTask, reply.ReduceTask = false, false
		c.mutex.Unlock()
		return nil
	}
	unNum, inNum := c.unstartedNum, c.inProgressNum
	c.mutex.Unlock()
	if unNum != 0 {
		reply.MapTask = true
	} else if unNum == 0 && inNum == 0 {
		reply.ReduceTask = true
	}
	return nil
}

func (c *Coordinator) MapTaskDeliver(args *MapArgs, reply *MapReply) error {
	// add lock here
	c.mutex.Lock()
	if c.unstartedNum == 0 {
		reply.Ok = false
		c.mutex.Unlock()
		return nil
	}
	chosed := c.unstartedFilesID[0]
	c.unstartedFilesID = c.unstartedFilesID[1:]
	c.unstartedNum--
	c.inProgressNum++
	c.mutex.Unlock()

	reply.MapID = chosed
	reply.Files = c.files[chosed : chosed+1]
	reply.NReduce = c.nReduce
	reply.Ok = true

	go func(chosed, ID int) {
		time.Sleep(10 * time.Second)
		c.mutex.Lock()
		if !c.finished[chosed] {
			c.noReplyWorker[ID] = true
			c.unstartedNum++
			c.inProgressNum--
			c.unstartedFilesID = append(c.unstartedFilesID, chosed)
			fmt.Printf("worker %v no reply, rebegin map task %v\n", ID, chosed)
		}
		c.mutex.Unlock()
	}(chosed, args.ID)

	return nil
}

func (c *Coordinator) MapEnd(args *MapEndArgs, reply *MapEndReply) error {
	c.mutex.Lock()
	c.noReplyWorker[args.ID] = false
	c.finished[args.MapID] = true
	c.inProgressNum--
	c.finishedNum++
	c.mutex.Unlock()
	reply.Success = true
	return nil
}

func (c *Coordinator) ReduceTaskDeliver(args *ReduceArgs, reply *ReduceReply) error {
	// add lock here
	c.mutex.Lock()
	reduceFinished, inPorgressReduce := c.finishReduce, c.inPorgressReduce
	if reduceFinished+inPorgressReduce == c.nReduce {
		i := 0
		for i < len(c.backupReduce) {
			if !c.finishedReduce[c.backupReduce[i]] {
				break
			}
			i++
		}
		if i < len(c.backupReduce) {
			reply.ReduceID = c.backupReduce[i]
			reply.Cnt = len(c.files)
			reply.Ok = true
			c.backupReduce = append(c.backupReduce[1:], c.backupReduce[i])
		} else {
			reply.Ok = false
		}
		c.mutex.Unlock()
		return nil
	}
	chosed := c.unstartedReduce[0]
	c.backupReduce = append(c.backupReduce, chosed)
	c.inPorgressReduce++
	c.unstartedReduce = c.unstartedReduce[1:]

	c.mutex.Unlock()

	reply.ReduceID = chosed
	reply.Cnt = len(c.files)
	reply.Ok = true

	go func(chosed, ID int) {
		time.Sleep(10 * time.Second)
		c.mutex.Lock()
		if !c.finishedReduce[chosed] {
			c.noReplyWorker[ID] = true
			c.inPorgressReduce--
			c.unstartedReduce = append(c.unstartedReduce, chosed)
			fmt.Printf("worker %v no reply, rebegin reduce task %v\n", ID, chosed)
		}
		c.mutex.Unlock()
	}(chosed, args.ID)

	return nil
}

func (c *Coordinator) ReduceEnd(args *ReduceEndArgs, reply *ReduceEndReply) error {
	c.mutex.Lock()
	c.noReplyWorker[args.ID] = false
	c.finishedReduce[args.ReduceID] = true
	c.inPorgressReduce--
	c.finishReduce++
	c.mutex.Unlock()
	reply.Success = true
	c.wg.Done()
	fmt.Printf("current finished reduce cnt: %v\n", c.finishReduce)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	c.wg.Wait()
	fmt.Println("all tasks finished")
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:            files,
		unstartedFilesID: make([]int, len(files)),
		unstartedReduce:  make([]int, nReduce),
		nReduce:          nReduce,
		unstartedNum:     len(files),
		inProgressNum:    0,
		finishedNum:      0,
		finished:         make(map[int]bool),
		finishReduce:     0,
		inPorgressReduce: 0,
		finishedReduce:   make(map[int]bool),
		noReplyWorker:    make(map[int]bool),
		nxtWorkerID:      0,
		backupReduce:     []int{},
	}
	c.wg.Add(nReduce)
	for i := 0; i < len(files); i++ {
		c.unstartedFilesID[i] = i
	}
	for i := 0; i < nReduce; i++ {
		c.unstartedReduce[i] = i
	}

	// Your code here.
	// 每次分配1~3个文件

	c.server()
	return &c
}
