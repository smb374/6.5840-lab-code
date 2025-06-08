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
	TmpDir string
}

type WorkerReduceReadyArgs struct{}
type WorkerReduceReadyReply struct {
	Mappers    int
	HasResched bool
}

type WorkerLeaveArgs struct {
	ID int
}
type WorkerLeaveReply struct{}

type GetMapTaskArgs struct {
	ID int
}
type GetMapTaskReply struct {
	ID          int
	FileName    string
	HasTaskLeft bool
}

type PostMapTaskArgs struct {
	ID    int
	MapID int
}
type PostMapTaskReply struct{}

type GetReduceTaskArgs struct {
	ID int
}
type GetReduceTaskReply struct {
	Split       int
	HasTaskLeft bool
}

type PostReduceTaskArgs struct {
	ID       int
	ReduceID int
}
type PostReduceTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
