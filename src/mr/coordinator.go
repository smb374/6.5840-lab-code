package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	NONE     TaskState = 0
	MAPPED   TaskState = 1
	FINISHED TaskState = 2
)
const TIME_LIMIT time.Duration = time.Minute

type Task struct {
	ID        int       // Task ID used in queue
	WorkerID  int       // Worker that takes this task
	State     TaskState // Task state
	StartTime time.Time // Task start time
}

type Coordinator struct {
	// Your definitions here.
	// States
	Files        []string // Files to process
	Maps         int      // #Maps
	Reduces      int      // #Reduces
	Workers      int      // #Workers
	MapPhaseDone bool     // Are all files mapped?
	Finished     bool     // Are all splits reduced?
	MapTasks     []Task   // Map Tasks
	ReduceTasks  []Task   // Reduce Tasks
	TmpDir       string

	// Task queues feeding Get* RPC calls
	MapQueue    chan int
	ReduceQueue chan int

	// Synchronization
	Lock      sync.Mutex
	MapCtx    context.Context
	ReduceCtx context.Context
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

// Watchdog
func (c *Coordinator) WatchDog(mcancel context.CancelFunc, rcancel context.CancelFunc) {
	for {
		c.Lock.Lock()
		if c.Finished {
			c.Lock.Unlock()
			return
		}
		if c.MapPhaseDone {
			finished := 0
			for i, task := range c.ReduceTasks {
				if task.State == MAPPED {
					now := time.Now()
					delta := now.Sub(task.StartTime)
					if delta > TIME_LIMIT {
						// Reschedule task
						c.ReduceTasks[i].State = NONE
						c.ReduceTasks[i].StartTime = time.Time{}

						c.ReduceQueue <- i
					}
				} else if task.State == FINISHED {
					finished += 1
				}
			}
			if finished == c.Reduces {
				c.Finished = true
				rcancel()
			}
		} else {
			finished := 0
			for i, task := range c.MapTasks {
				if task.State == MAPPED {
					now := time.Now()
					delta := now.Sub(task.StartTime)
					if delta > TIME_LIMIT {
						// Reschedule task
						c.MapTasks[i].State = NONE
						c.MapTasks[i].StartTime = time.Time{}

						c.MapQueue <- i
					}
				} else if task.State == FINISHED {
					finished += 1
				}
			}
			if finished == c.Maps {
				c.MapPhaseDone = true
				for i := range c.Reduces {
					c.ReduceQueue <- i
				}

				mcancel()
			}
		}
		c.Lock.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) WorkerJoin(args *WorkerJoinArgs, reply *WorkerJoinReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	reply.ID = c.Workers
	reply.Splits = c.Reduces
	reply.TmpDir = c.TmpDir

	c.Workers += 1

	return nil
}

func (c *Coordinator) WorkerLeave(args *WorkerLeaveArgs, reply *WorkerLeaveReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// Reschedule tasks
	// No need to change things other than state & start time.
	if c.MapPhaseDone {
		for i, task := range c.ReduceTasks {
			if task.WorkerID == args.ID && task.State == MAPPED {
				c.ReduceTasks[i].State = NONE
				c.ReduceTasks[i].StartTime = time.Time{}
				c.ReduceQueue <- i
			}
		}
	} else {
		for i, task := range c.MapTasks {
			if task.WorkerID == args.ID && task.State == MAPPED {
				c.MapTasks[i].State = NONE
				c.MapTasks[i].StartTime = time.Time{}
				c.MapQueue <- i
			}
		}
	}
	return nil
}

func (c *Coordinator) WorkerReduceReady(args *WorkerReduceReadyArgs, reply *WorkerReduceReadyReply) error {
	reply.Mappers = c.Maps
	return nil
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.Lock.Lock()

	if c.MapPhaseDone || c.Finished {
		reply.HasTaskLeft = false
		c.Lock.Unlock()
		return nil
	}

	c.Lock.Unlock()

	for {
		select {
		case idx := <-c.MapQueue:
			c.Lock.Lock()
			if c.MapTasks[idx].State != NONE {
				c.Lock.Unlock()
				continue
			}
			c.MapTasks[idx].WorkerID = args.ID
			c.MapTasks[idx].State = MAPPED
			c.MapTasks[idx].StartTime = time.Now()
			c.Lock.Unlock()

			reply.ID = idx
			reply.FileName = c.Files[idx]
			reply.HasTaskLeft = true
			return nil
		case <-c.MapCtx.Done():
			reply.HasTaskLeft = false
			return nil
		}
	}
}

func (c *Coordinator) PostMapTask(args *PostMapTaskArgs, reply *PostMapTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.MapTasks[args.MapID].WorkerID == args.ID {
		c.MapTasks[args.MapID].State = FINISHED
	} else {
		log.Printf("Got stale map task %v from worker %v.", args.MapID, args.ID)
	}
	return nil
}

func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.Lock.Lock()

	if c.Finished {
		reply.HasTaskLeft = false
		c.Lock.Unlock()
		return nil
	}

	c.Lock.Unlock()

	for {
		select {
		case idx := <-c.ReduceQueue:
			c.Lock.Lock()
			if c.ReduceTasks[idx].State != NONE {
				c.Lock.Unlock()
				continue
			}
			c.ReduceTasks[idx].WorkerID = args.ID
			c.ReduceTasks[idx].State = MAPPED
			c.ReduceTasks[idx].StartTime = time.Now()
			c.Lock.Unlock()

			reply.Split = idx
			reply.HasTaskLeft = true
			return nil
		case <-c.ReduceCtx.Done():
			reply.HasTaskLeft = false
			return nil
		}
	}
}

func (c *Coordinator) PostReduceTask(args *PostReduceTaskArgs, reply *PostReduceTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.ReduceTasks[args.ReduceID].WorkerID == args.ID {
		c.ReduceTasks[args.ReduceID].State = FINISHED
	} else {
		log.Printf("Got stale reduce task %v from worker %v.", args.ReduceID, args.ID)
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Lock.Lock()
	defer c.Lock.Unlock()

	ret = c.Finished

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.Maps = len(files)
	c.Reduces = nReduce
	c.Workers = 0
	c.Lock = sync.Mutex{}
	mctx, mcancel := context.WithCancel(context.Background())
	rctx, rcancel := context.WithCancel(context.Background())
	c.MapCtx = mctx
	c.ReduceCtx = rctx
	tmp, err := os.MkdirTemp("", "mr-tmp")
	if err != nil {
		tmp = "."
		log.Printf("Failed to create tmp dir for intermediate files: %v", err)
	}
	c.TmpDir = tmp

	c.MapTasks = make([]Task, c.Maps)
	c.ReduceTasks = make([]Task, c.Reduces)

	c.MapQueue = make(chan int, c.Maps)
	c.ReduceQueue = make(chan int, c.Reduces)

	for i := range c.Maps {
		c.MapTasks[i] = Task{
			ID:        i,
			WorkerID:  0,
			State:     NONE,
			StartTime: time.Time{},
		}

		c.MapQueue <- i
	}

	for i := range c.Reduces {
		c.ReduceTasks[i] = Task{
			ID:        i,
			WorkerID:  0,
			State:     NONE,
			StartTime: time.Time{},
		}
	}

	c.server()
	go c.WatchDog(mcancel, rcancel)
	return &c
}
