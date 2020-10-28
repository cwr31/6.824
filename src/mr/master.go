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

const TIME_OUT = time.Second * 5

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
	sch      chan bool
}

func (m *Master) Register(req *RegisterReq, res *RegisterRes) error {
	log.Printf("Register lock = %+v", m.mu)
	m.mu.Lock()
	defer m.mu.Unlock()

	res.WorkerId = m.NextWorkerId
	log.Printf("[master] Worker Register, Assign Id %d", m.NextWorkerId)
	m.NextWorkerId++
	return nil
}

func (m *Master) AcquireTask(req *AcquireTaskReq, res *AcquireTaskRes) error {
	log.Printf("AcquireTask lock = %+v", m.mu)
	res.WorkerId = req.WorkerId
	res.Task = <-m.Tasks

	m.mu.Lock()
	defer m.mu.Unlock()
	// 在master中保存task状态
	m.TaskList[res.Task.Id].WorkerId = req.WorkerId
	m.TaskList[res.Task.Id].Status = assigned
	m.TaskList[res.Task.Id].StartTime = time.Now()
	return nil
}

func (m *Master) UpdateTaskStatus(req *UpdateTaskStateReq, res *UpdateTaskStateRes) error {
	log.Printf("UpdateTaskStatus lock = %+v", m.mu)
	m.mu.Lock()
	defer m.mu.Unlock()

	res.WorkerId = req.WorkerId
	m.TaskList[req.TaskId].Status = req.TaskState
	log.Printf("[master] Task %d status updated to %d", req.TaskId, req.TaskState)

	//go m.schedule()
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
	//m.TaskList = make([]Task, len(files)+nReduce)
	for taskId, filename := range files {
		m.TaskList = append(m.TaskList, Task{
			Id:        taskId,
			Type:      _map,
			InputName: filename,
			NMap:      len(files),
			NReduce:   nReduce,
		})
	}
	m.sch = make(chan bool, 1)
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
	return m.done
}

func (m *Master) tickSchedule() {
	for {
		log.Printf("m.Done = %v", m.Done())
		if !m.Done() {
			log.Printf("just tick it")
			go m.schedule()
			//<- m.sch
			time.Sleep(time.Millisecond * 1000)
		}
	}
}

func (m *Master) schedule() {
	log.Printf("schedule lock = %+v", m.mu)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Done() {
		return
	}

	log.Printf("[master] taskList size = %d", len(m.TaskList))
	completedMapTasks := 0
	completedTasks := 0
	for taskId := range m.TaskList[0:m.NMap] {
		if m.TaskList[taskId].Status == completed {
			completedMapTasks++
		}
	}
	log.Printf("[master] completedMapTasks = %d, NMap = %d", completedMapTasks, m.NMap)
	if completedMapTasks == m.NMap && m.phase == map_phase {
		m.phase = reduce_phase
		for i := 0; i < m.NReduce; i++ {
			m.TaskList = append(m.TaskList, Task{
				Id:          m.NMap + i,
				Type:        _reduce,
				InputName:   "",
				NMap:        m.NMap,
				ReduceIndex: i,
				Status:      unassigned,
			})
		}
	}
	for taskId, task := range m.TaskList {
		switch task.Status {
		case unassigned:
			m.Tasks <- task
			m.TaskList[taskId].Status = queued
			break
		case queued:
			break
		case assigned:
			if time.Now().Sub(task.StartTime) > TIME_OUT {
				log.Printf("[master] Task %d timeout, reassign...", taskId)
				m.Tasks <- task
				m.TaskList[taskId].Status = queued
			}
			break
		case completed:
			completedTasks++
			break
		case failed:
			m.Tasks <- task
			m.TaskList[taskId].Status = queued
			break
		}
	}
	log.Printf("[master] completedTasks = %d", completedTasks)
	if completedTasks == m.NMap+m.NReduce {
		log.Printf("[master] done")
		m.done = true
	}
	//m.sch <- true
}
