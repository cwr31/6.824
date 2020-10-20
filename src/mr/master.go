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

var mapTasks chan string
var reduceTasks chan string

type Master struct {
	// Your definitions here.
	MapTasks     map[string]int
	ReduceTasks  map[int]int
	NextWorkerId int
}

type TaskInfo struct {
	_type int
	state int

	fileName  string
	fileIndex int
	nReduce   int
	nFiles    int
	startTime time.Time
}

type TaskInterface interface {
	getTaskInfo() TaskInfo
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

func (this *MapTaskInfo) getTaskInfo() TaskInfo {
	return TaskInfo{
		_type:     0,
		state:     0,
		fileName:  this.fileName,
		fileIndex: this.fileIndex,
		nReduce:   this.nReduce,
		nFiles:    this.nFiles,
	}
}

func (this *ReduceTaskInfo) getTaskInfo() TaskInfo {
	return TaskInfo{
		_type:     0,
		state:     0,
		fileName:  this.fileName,
		fileIndex: this.fileIndex,
		nReduce:   this.nReduce,
		nFiles:    this.nFiles,
	}
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
		m.MapTasks[v] = unassigned
	}
	m.NextWorkerId = 0
	//for i := 0; i<nReduce; i++{
	//	m.ReduceTasks[i] = unassigned
	//}

	// Your code here.

	m.server()
	return &m
}
