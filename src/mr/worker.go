package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type infoWorker struct {
	ID int
}

var info infoWorker = infoWorker{}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	CallForID()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		state, received := CallForTask()
		fmt.Printf("worker %v beg for a task and returned with %v;\n", info.ID, state)
		if !received {
			fmt.Printf("worker exits\n")
			break
		}
		if state.MapTask {
			reply := CallForMapTask()
			if !reply.Ok {
				fmt.Println("no map task can be assigned currently!")
				continue
			}
			intermediate := make([][]KeyValue, reply.NReduce)
			for _, filename := range reply.Files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read file %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				for _, kv := range kva {
					fileIndex := ihash(kv.Key) % reply.NReduce
					intermediate[fileIndex] = append(intermediate[fileIndex], kv)
				}
			}
			flag := outputIntermediate(intermediate, reply)
			if !flag {
				continue
			}
			ok := call("Coordinator.MapEnd", &MapEndArgs{MapID: reply.MapID}, &MapEndReply{})
			if !ok {
				fmt.Printf("mapEndSignal failed!\n")
			} else {
				fmt.Printf("worker %v finished map task!\n", info.ID)
			}
		} else if state.ReduceTask {
			reply := CallForReduceTask()
			if !reply.Ok {
				continue
			}
			kva := []KeyValue{}
			for i := 0; i < reply.Cnt; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))

			fmt.Printf("ReduceID: %v\nv: %v\n", reply.ReduceID, kva)

			tmpfile, err := ioutil.TempFile("", "tmp-o-*-"+string(time.Now().Unix()))
			if err != nil {
				log.Fatalf("error for tmpfile %v\n", err)
			}

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}

				fmt.Printf("ReduceID: %v\nv: %v, ptr: %v %v\n", reply.ReduceID, values, j, len(kva))

				output := reducef(kva[i].Key, values)

				fmt.Printf("ReduceID: %v\noutput : %v\n", reply.ReduceID, output)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			tmpfile.Close()

			outfilename := "mr-out-" + strconv.Itoa(reply.ReduceID)
			if ok, _ := PathExists(outfilename); ok {
				fmt.Printf("file %v already exists!\n", outfilename)
				err := os.Remove(tmpfile.Name())
				if err != nil {
					fmt.Printf("remove tmp failed!")
				}
				continue
			}
			fmt.Printf("rename tmpfile to %v\n", outfilename)
			err = os.Rename(tmpfile.Name(), outfilename)
			fmt.Printf("rename to %v succeed\n", outfilename)

			ok := call("Coordinator.ReduceEnd", &ReduceEndArgs{ID: info.ID, ReduceID: reply.ReduceID}, &ReduceEndReply{})
			if !ok {
				fmt.Printf("reduceEndSignal failed! Maybe mr job has been fininshed\n")
				fmt.Printf("worker exits\n")
				return
			}
			fmt.Printf("worker %v finished reduce ID: %v\n", info.ID, reply.ReduceID)

		}
		time.Sleep(500 * time.Millisecond)
	}

}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// intermediate Map output storage
func outputIntermediate(intermediate [][]KeyValue, reply MapReply) bool {
	fmt.Printf("worker %v begin write intermediate to file\n", info.ID)
	filename := "mr-" + strconv.Itoa(reply.MapID) + "-"
	for i := 0; i < reply.NReduce; i++ {
		curFilename := filename + strconv.Itoa(i)
		tmpfile, err := ioutil.TempFile("", "tmp-*-"+string(time.Now().Unix()))
		// file, err := os.Create(curFilename)
		if err != nil {
			log.Fatalf("cannot create tmp file %v\n", tmpfile)
		}
		enc := json.NewEncoder(tmpfile)
		sort.Sort(ByKey(intermediate[i]))
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot store kv pair into %v in json\n", curFilename)
			}
		}
		tmpfile.Close()
		if ok, _ := PathExists(curFilename); ok {
			os.Remove(tmpfile.Name())
			fmt.Printf("file %v already exists!\n", curFilename)
			return false
		}
		err = os.Rename(tmpfile.Name(), curFilename)
		fmt.Printf("file %v gengerated\n", curFilename)
		if err != nil {
			log.Fatalf("cannot rename to %v", curFilename)
		}
	}
	return true
}

func CallForTask() (TaskReply, bool) {
	args := TaskArgs{ID: info.ID}
	reply := TaskReply{}
	ok := call("Coordinator.TaskAlready", &args, &reply)
	if ok {
		// fmt.Printf("call for task failed\n")
	} else {
		fmt.Printf("call failed! Maybe mr job has been fininshed\n")
	}
	return reply, ok
}

// call for task
func CallForMapTask() MapReply {
	args := MapArgs{ID: info.ID}
	reply := MapReply{}

	ok := call("Coordinator.MapTaskDeliver", &args, &reply)
	if ok {
		fmt.Printf("replay with input files: %s\n", strings.Join(reply.Files, ","))
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallForReduceTask() ReduceReply {
	args := ReduceArgs{ID: info.ID}
	reply := ReduceReply{}
	ok := call("Coordinator.ReduceTaskDeliver", &args, &reply)
	if ok {
		fmt.Printf("replay with reduce ID: %v(%v)\n", reply.ReduceID, reply.Ok)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallForID() {
	args := InitArgs{}
	reply := InitReply{}
	ok := call("Coordinator.InitWorker", &args, &reply)
	if ok {
		fmt.Printf("worker init successed with ID: %v\n", reply.ID)
	} else {
		fmt.Println("worker init failed")
	}
	info.ID = reply.ID
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
*/
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
