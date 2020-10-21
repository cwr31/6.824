package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	unassigned = iota
	assigned
	completed
)

const (
	_map = iota
	_reduce
)

type Master struct {
	// Your definitions here.
	NextWorkerId int
	NReduce      int
	MapTasks     chan string
	reduceTasks  chan string
}

type TaskInfo struct {
	Type       int
	StartTime  time.Time
	State      int
	InputName  string
	OutputName string
}

type TaskInterface interface {
	getNextTask() TaskInfo
	timeout() bool
	getFileIndex() int
	getPartIndex() int
	setNow()
}

type MapTaskInfo struct {
	TaskInfo
}

type ReduceTaskInfo struct {
	TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Register(args *RegisterRequest, reply *RegisterResponse) error {
	m.NextWorkerId++
	reply.WorkerId = m.NextWorkerId
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.MapTasks = make(map[string]int)
	// one file correspond to one map task
	for _, v := range files {
		m.MapTasks <- v
	}
	m.NextWorkerId = 0

	// Your code here.

	m.server()
	return &m
}
