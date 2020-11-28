package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MyWorker struct {
	WorkerId    int
	CurrentTask Task
	Mapf        func(string, string) []KeyValue
	Reducef     func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := MyWorker{}
	w.Mapf = mapf
	w.Reducef = reducef
	w.Register()
	w.run()
	//w.processReduceTask(Task{
	//		Type:        0,
	//		Id:          0,
	//		InputName:   "",
	//		NMap:        1,
	//		OutputName:  "",
	//		ReduceIndex: 0,
	//		NReduce:     0,
	//	})
}

func (w *MyWorker) run() {
	for {
		t, err := w.AcquireTask()
		if err != nil {
			log.Printf(err.Error())
		}
		processTaskOK := w.processTask(t)
		log.Printf("processTaskOK = %t", processTaskOK)
		if processTaskOK {
			w.UpdateTaskStatus(t.Id, completed)
		} else {
			w.UpdateTaskStatus(t.Id, failed)
		}
		time.Sleep(time.Second)
	}
}

func (w *MyWorker) processTask(info Task) bool {
	switch info.Type {
	case _map:
		return w.processMapTask(info)
	case _reduce:
		return w.processReduceTask(info)
	}
	return false
}

func (w *MyWorker) processMapTask(info Task) bool {
	log.Printf("[worker-%d] processMapTask start, InputName = %+v", w.WorkerId, info)
	contents, err := ioutil.ReadFile(info.InputName)
	if err != nil {
		log.Printf("ReadFile not ok, %s", err)
		return false
	}
	kvs := w.Mapf(info.InputName, string(contents))
	reduces := make([][]KeyValue, info.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % info.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	for index, kv := range reduces {
		fileName :=
			fmt.Sprintf("mr-%d-%d", info.Id, index)
		file, err := ioutil.TempFile("/Users/cwr/Desktop/projects/6.824/src/main/mr-tmp", "map-")
		if err != nil {
			log.Printf("Create file error")
			return false
		}
		buf := bufio.NewWriter(file)
		for _, v := range kv {
			buf.WriteString(v.Key + "," + v.Value + "\n")
		}
		if buf.Flush() != nil {
			log.Printf("Flush file error")
			return false
		}
		if file.Close() != nil {
			log.Printf("Close file error")
			return false
		}
		os.Rename(file.Name(), fileName)
	}
	return true
}

func (w *MyWorker) processReduceTask(info Task) bool {
	log.Printf("[worker-%d] processReduceTask start, InputName = %+v", w.WorkerId, info)
	maps := make(map[string][]string)
	for i := 0; i < info.NMap; i++ {
		inputFileName := fmt.Sprintf("mr-%d-%d", i, info.ReduceIndex)
		ifile, err := os.Open(inputFileName)
		if err != nil {
			return false
		}
		buf := bufio.NewReader(ifile)
		for {
			line, _, err := buf.ReadLine()
			if err == io.EOF {
				break
			}
			s := strings.Split(string(line), ",")
			k := s[0]
			v := s[1]
			maps[k] = append(maps[k], v)
		}
	}
	outputFileName := fmt.Sprintf("mr-out-%d", info.ReduceIndex)
	log.Printf("[worker-%d] outputFileName=%s", w.WorkerId, outputFileName)
	ofile, _ :=
		ioutil.TempFile("/Users/cwr/Desktop/projects/6.824/src/main/mr-tmp", "reduce-")
	for k, v := range maps {
		fmt.Fprintf(ofile, "%v %v\n", k, w.Reducef(k, v))
	}
	os.Rename(ofile.Name(), outputFileName)
	return true
}

func (w *MyWorker) UpdateTaskStatus(TaskId int, TaskState int) {
	req := UpdateTaskStateReq{
		WorkerId:  w.WorkerId,
		TaskId:    TaskId,
		TaskState: TaskState,
	}
	res := UpdateTaskStateRes{}
	ok := call("Master.UpdateTaskStatus", &req, &res)
	if ok && res.WorkerId == w.WorkerId {
		log.Printf("[worker-%d] Update Task %d Status succeed, res = %+v", w.WorkerId, req.TaskId, res)
	}
}

func (w *MyWorker) Register() {
	req := RegisterReq{}
	res := RegisterRes{}
	ok := call("Master.Register", &req, &res)
	if ok && res.WorkerId == w.WorkerId {
		w.WorkerId = res.WorkerId
		log.Println("Register succeed, res = ", res)
	}
}

func (w *MyWorker) AcquireTask() (Task, error) {
	req := AcquireTaskReq{
		WorkerId: w.WorkerId,
	}
	res := AcquireTaskRes{}
	ok := call("Master.AcquireTask", &req, &res)
	log.Printf("[worker-%d] AcquireTask ok = %t, res = %+v", w.WorkerId, ok, res)
	if ok && res.WorkerId == w.WorkerId {
		log.Println("AcquireTask succeed, res = ", res)
		return res.Task, nil
	}
	return Task{}, errors.New("AcquireTask failed")
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
