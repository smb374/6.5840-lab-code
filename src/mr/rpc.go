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
type WorkerJoinArgs struct{}
type WorkerJoinReply struct {
	ID     int
	Splits int
	IsFull bool
}

type WorkerReduceReadyArgs struct{}
type WorkerReduceReadyReply struct {
	Mappers int
}

type WorkerLeaveArgs struct {
	ID int
}
type WorkerLeaveReply struct{}

type GetMapTaskArgs struct{}
type GetMapTaskReply struct {
	ID          int
	FileName    string
	HasTaskLeft bool
}

type PostMapTaskArgs struct {
	ID       int
	FileName string
}
type PostMapTaskReply struct {
	HasTaskLeft bool
}

type GetReduceSplitArgs struct{}
type GetReduceSplitReply struct {
	Split        int
	HasSplitLeft bool
}

type PostReduceSplitArgs struct {
	Split int
}
type PostReduceSplitReply struct {
	HasSplitLeft bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
