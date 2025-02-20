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
type HeartBeatArgs struct {
	
}
type HeartBeatReply struct {
	//Phase int
}
type  CheckPhaseArgs struct {
	//Phase int
}
type CheckPhaseReply struct {
	Phase int
}
type DoneReportArgs struct {
	TaskId int
}
type DoneReportReply struct {
	
}
type HeartbeatArgs struct {
	Task *Task
}
type HeartbeatReply struct {
	Alive bool
}
//
type AllocateTaskArgs struct {
	//WorkType int // 0: map, 1: reduce
}

type AllocateTaskReply struct {
	Task *Task
	Lenfiles int
}

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
