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

type RegisterReq struct {
}

type RegisterRes struct {
	WorkerId int
}

type AcquireTaskReq struct {
	WorkerId int
}

type AcquireTaskRes struct {
	WorkerId int
	Task     Task
}

type UpdateTaskStateReq struct {
	WorkerId  int
	TaskId    int
	TaskState int
}

type UpdateTaskStateRes struct {
	WorkerId int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
