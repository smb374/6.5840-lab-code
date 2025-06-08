package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type EncFile struct {
	File *os.File
	Enc  *json.Encoder
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// Worker join to system
	wparam, ok := CallWorkerJoin()
	if !ok {
		return
	}
	// Worker Leave on return
	defer CallWorkerLeave(wparam.ID)

	// Map phase
	for {
		// Get map task
		task, ok := CallGetMapTask(wparam.ID)
		if !ok || !task.HasTaskLeft {
			break
		}
		// Read content
		file, err := os.Open(task.FileName)
		if err != nil {
			log.Fatalf("Failed to open task file: %v", err)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read task file: %v", err)
		}
		file.Close()
		intermediate := make(map[int]EncFile)
		// Map the task file
		kva := mapf(task.FileName, string(content))
		for _, kv := range kva {
			idx := ihash(kv.Key) % wparam.Splits

			if _, ok := intermediate[idx]; !ok {
				// Open file & create encoder
				fpath := fmt.Sprintf("./mr-%v-%v", task.ID, idx)
				file, err := os.OpenFile(fpath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("Failed to open intermediate file: %v", err)
				}
				intermediate[idx] = EncFile{
					File: file,
					Enc:  json.NewEncoder(file),
				}
			}

			intermediate[idx].Enc.Encode(&kv)
		}

		for _, ef := range intermediate {
			ef.File.Close()
		}

		// Post Task Finished Message
		_, ok = CallPostMapTask(wparam.ID, task.ID)
		if !ok {
			break
		}
	}

	// Reduce Phase
	// Wait other workers to be ready for reduce.
	ready, ok := CallWorkerReduceReady()
	if !ok {
		return
	}

	// Read assigned splits from the coordinator
	for {
		rparam, ok := CallGetReduceTask(wparam.ID)
		if !ok || !rparam.HasTaskLeft {
			break
		}
		oname := fmt.Sprintf("mr-out-%v", rparam.Split)
		ofile, _ := os.Create(oname)

		// Read whole split into kvs
		kvs := []KeyValue{}
		for i := range ready.Mappers {
			// Read intermediate file
			fname := fmt.Sprintf("./mr-%v-%v", i, rparam.Split)
			file, err := os.Open(fname)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				log.Fatalf("Failed to open intermediate file: %v", err)
			}
			// Decode JSON lines to KeyValue
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					if err == io.EOF {
						break
					}

					log.Fatalf("Failed to decode kv: %v", err)
				}

				kvs = append(kvs, kv)
			}

			file.Close()
		}

		// Sort & Reduce to the output file
		sort.Sort(ByKey(kvs))

		i := 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := reducef(kvs[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

			i = j
		}

		ofile.Close()

		// Post Split process done
		_, ok = CallPostReduceTask(wparam.ID, rparam.Split)
		if !ok {
			break
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func CallWorkerJoin() (rep WorkerJoinReply, ok bool) {
	args := WorkerJoinArgs{}
	ok = call("Coordinator.WorkerJoin", &args, &rep)
	return
}

func CallWorkerReduceReady() (rep WorkerReduceReadyReply, ok bool) {
	args := WorkerReduceReadyArgs{}
	ok = call("Coordinator.WorkerReduceReady", &args, &rep)
	return
}

func CallWorkerLeave(id int) (ok bool) {
	args := WorkerLeaveArgs{
		ID: id,
	}
	rep := WorkerLeaveReply{}
	ok = call("Coordinator.WorkerLeave", &args, &rep)
	return
}

func CallGetMapTask(id int) (rep GetMapTaskReply, ok bool) {
	args := GetMapTaskArgs{
		ID: id,
	}

	ok = call("Coordinator.GetMapTask", &args, &rep)
	return
}

func CallPostMapTask(id int, mid int) (rep PostMapTaskReply, ok bool) {
	args := PostMapTaskArgs{
		ID:    id,
		MapID: mid,
	}

	ok = call("Coordinator.PostMapTask", &args, &rep)
	return
}

func CallGetReduceTask(id int) (rep GetReduceTaskReply, ok bool) {
	args := GetReduceTaskArgs{
		ID: id,
	}
	ok = call("Coordinator.GetReduceTask", &args, &rep)
	return
}

func CallPostReduceTask(id int, rid int) (rep PostReduceTaskReply, ok bool) {
	args := PostReduceTaskArgs{
		ID:       id,
		ReduceID: rid,
	}

	ok = call("Coordinator.PostReduceTask", &args, &rep)
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
