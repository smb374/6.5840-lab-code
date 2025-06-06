package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	Files   []string
	TaskIdx int
	Workers int
	Splits  int
	Mapped  int
	Lock    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerJoin(args *WorkerJoinArgs, reply *WorkerJoinReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if c.Mapped == len(c.Files) || c.Workers == c.Splits {
		reply.IsFull = true
		return nil
	}

	reply.ID = c.Workers
	reply.Splits = c.Splits

	c.Workers += 1
	return nil
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if args.ID >= c.Workers {
		return fmt.Errorf("Invalid worker %v", args.ID)
	}

	if c.TaskIdx == len(c.Files) {
		reply.HasTaskLeft = false
		return nil
	}

	reply.FileName = c.Files[c.TaskIdx]
	c.TaskIdx += 1
	return nil
}

func (c *Coordinator) PostMapTask(args *PostMapTaskArgs, reply *PostMapTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if args.ID >= c.Workers {
		return fmt.Errorf("Invalid worker %v", args.ID)
	}

	reply.HasTaskLeft = c.TaskIdx == len(c.Files)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.Splits = nReduce

	c.server()
	return &c
}
