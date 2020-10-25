package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	unassigned = iota
	queued
	assigned
	completed
	failed
)

const TIME_OUT = time.Second * 60

const (
	_map = iota
	_reduce
)

const (
	map_phase = iota
	reduce_phase
)

type Task struct {
	Type        int
	Id          int
	InputName   string
	NMap        int
	OutputName  string
	ReduceIndex int
	NReduce     int
	WorkerId    int
	Status      int
	StartTime   time.Time
}

type Master struct {
	// Your definitions here.
	mu           sync.Mutex
	NextWorkerId int
	NMap         int
	NReduce      int
	Tasks        chan Task
	// taskId and status
	TaskList []Task
	done     bool
	phase    int
}

func (m *Master) Register(req *RegisterReq, res *RegisterRes) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	res.WorkerId = m.NextWorkerId
	log.Printf("[master] Worker Register, Assign Id %d", m.NextWorkerId)
	m.NextWorkerId++
	return nil
}

func (m *Master) AcquireTask(req *AcquireTaskReq, res *AcquireTaskRes) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	res.WorkerId = req.WorkerId
	res.Task = <-m.Tasks
	// 在master中保存task状态
	m.TaskList[res.Task.Id].WorkerId = req.WorkerId
	m.TaskList[res.Task.Id].Status = assigned
	m.TaskList[res.Task.Id].StartTime = time.Now()
	return nil
}

func (m *Master) UpdateTaskStatus(req *UpdateTaskStateReq, res *UpdateTaskStateRes) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	res.WorkerId = req.WorkerId
	m.TaskList[req.TaskId].Status = req.TaskState
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
		m.Tasks = make(chan Task, len(files))
	} else {
		m.Tasks = make(chan Task, nReduce)
	}
	m.mu = sync.Mutex{}
	m.TaskList = make([]Task, len(files)+nReduce)
	for taskId, filename := range files {
		m.TaskList = append(m.TaskList, Task{
			Id:        taskId,
			Type:      _map,
			InputName: filename,
			NMap:      len(files),
			NReduce:   nReduce,
		})
	}
	m.NMap = len(files)
	m.NextWorkerId = 0
	m.NReduce = nReduce
	m.server()
	go m.tickSchedule()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) tickSchedule() {
	for {
		if !m.Done() {
			go m.schedule()
			time.Sleep(time.Millisecond * 500)
		}
	}
}

func (m *Master) schedule() {
	completedMapTasks := 0
	completedTasks := 0
	for _, task := range m.TaskList[0 : m.NMap-1] {
		if task.Status == completed {
			completedMapTasks++
		}
	}
	if completedMapTasks == m.NMap {
		for i := 0; i < m.NReduce; i++ {
			m.TaskList = append(m.TaskList, Task{
				Id:          m.NMap - 1 + i,
				Type:        _reduce,
				InputName:   "",
				NMap:        m.NMap,
				ReduceIndex: i,
				Status:      unassigned,
			})
		}
	}
	for _, task := range m.TaskList {
		switch task.Status {
		case unassigned:
			m.Tasks <- task
			task.Status = queued
			break
		case queued:
			break
		case assigned:
			if time.Now().Sub(task.StartTime) > TIME_OUT {
				m.Tasks <- task
			}
			break
		case completed:
			completedTasks++
			break
		case failed:
			m.Tasks <- task
			task.Status = queued
			break
		}
	}
	if completedTasks == m.NMap+m.NReduce {
		m.done = true
	}
}
