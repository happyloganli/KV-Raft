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

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type TaskStatus int

const (
	idle TaskStatus = iota
	inProgress
	completed
)

type Coordinator struct {
	files       []string
	nReduce     int
	nMap        int
	mapTasks    map[int]Task
	reduceTasks map[int]Task
	taskLock    sync.Mutex
	// Channels for handling messages and completion signals.
	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// Assign a task to a worker.
func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskResponse) error {
	// Locks the task data structure and ensures it is unlocked when the function returns.
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	for id, task := range c.mapTasks {
		if task.status == idle {
			task.status = inProgress
			task.startTime = time.Now()
			c.mapTasks[id] = task

			reply.TaskType = "map"
			reply.TaskID = id
			reply.FileName = task.fileName
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
	}

	for id, task := range c.reduceTasks {
		if task.status == idle {
			task.status = inProgress
			task.startTime = time.Now()
			c.reduceTasks[id] = task

			reply.TaskType = "reduce"
			reply.TaskID = id
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
	}

	if c.isDone() {
		reply.TaskType = "exit"
	} else {
		reply.TaskType = "wait"
	}
	return nil
}

// Report task completion by a worker.
func (c *Coordinator) ReportTask(args *ReportRequest, reply *ReportResponse) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if args.TaskType == "map" {
		task := c.mapTasks[args.TaskID]
		task.status = completed
		c.mapTasks[args.TaskID] = task
	} else if args.TaskType == "reduce" {
		task := c.reduceTasks[args.TaskID]
		task.status = completed
		c.reduceTasks[args.TaskID] = task
	}

	reply.Success = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.isDone()
}

func (c *Coordinator) isDone() bool {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	for _, task := range c.mapTasks {
		if task.status != completed {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.status != completed {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		mapTasks:    make(map[int]Task),
		reduceTasks: make(map[int]Task),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			fileName: file,
			id:       i,
			status:   idle,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			id:     i,
			status: idle,
		}
	}

	c.server()
	return &c
}
