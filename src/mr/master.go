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
	fail
)

const (
	_map = iota
	_reduce
)

type TaskInfo struct {
	Type       int
	Id         int
	StartTime  time.Time
	State      int
	InputName  string
	OutputName string
	NReduce    int
}

type Master struct {
	// Your definitions here.
	NextWorkerId int
	NReduce      int
	UniqueTaskId int
	Tasks        chan TaskInfo
	TaskState    []int
}

func (m *Master) Register(req *RegisterReq, res *RegisterRes) error {
	res.WorkerId = m.NextWorkerId
	log.Printf("[master] Worker Register, Assign Id %d", m.NextWorkerId)
	m.NextWorkerId++
	return nil
}

func (m *Master) AcquireTask(req *AcquireTaskReq, res *AcquireTaskRes) error {
	res.WorkerId = req.WorkerId
	res.TaskInfo = <-m.Tasks
	res.TaskInfo.Id = m.UniqueTaskId
	m.UniqueTaskId++
	m.TaskState[m.UniqueTaskId] = assigned
	return nil
}

func (m *Master) UpdateTaskState(req *UpdateTaskStateReq, res *UpdateTaskStateRes) error {
	res.WorkerId = req.WorkerId
	m.TaskState[req.TaskId] = req.TaskState
	log.Printf("[master] Task %d state updated to %d", req.TaskId, req.TaskState)
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	if len(files) > nReduce {
		m.Tasks = make(chan TaskInfo, len(files))
	} else {
		m.Tasks = make(chan TaskInfo, nReduce)
	}
	for _, v := range files {
		m.Tasks <- TaskInfo{
			Type:      _map,
			State:     unassigned,
			InputName: v,
			NReduce:   nReduce,
		}
	}
	m.TaskState = make([]int, len(files)*(nReduce+1))
	m.NextWorkerId = 0
	m.NReduce = nReduce
	// Your code here.
	m.server()
	return &m
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
