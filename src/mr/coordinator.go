package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//master kept task states
//<-------------------------------------
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

//------------------------------------->

//task
//<-------------------------------------
type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type TaskRet int

const (
	Success TaskRet = iota
	Failed
)

type TaskId int

//map or reduce task
type Task struct {
	Id        TaskId
	Type      TaskType
	State     TaskState
	Ret       *TaskRet
	InputFile string
}

//------------------------------------->

//task,map or reduce task
//<-------------------------------------
type WorkerId int

type TaskMeta struct {
	WorkerId  *WorkerId
	StartTime *time.Time
	EndTime   *time.Time
	TaskRef   *Task
}

//------------------------------------->

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	CompletedPhase
)

type TaskIdSeed int

type Coordinator struct {
	InputFiles    []string
	NReduce       int
	TaskQueue     chan *Task
	ExcecutePhase Phase
	TaskMapper    map[TaskId]*TaskMeta
	TaskIdSeed    TaskId
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ExcecutePhase == CompletedPhase
}

func (c *Coordinator) generateTaskId() TaskId {
	c.TaskIdSeed++
	return c.TaskIdSeed
}

func (c *Coordinator) createMapTask(files []string) {
	for _, file := range files {
		task := Task{
			Id:        c.generateTaskId(),
			Type:      Map,
			State:     Idle,
			InputFile: file,
		}
		c.TaskQueue <- &task
		now := time.Now()
		meta := TaskMeta{
			StartTime: &now,
			TaskRef:   &task,
		}
		c.TaskMapper[task.Id] = &meta
	}
}

func max(left int, right int) int {
	if left > right {
		return left
	}
	return right
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:    files,
		NReduce:       nReduce,
		TaskQueue:     make(chan *Task, max(len(files), nReduce)),
		ExcecutePhase: MapPhase,
		TaskMapper:    make(map[TaskId]*TaskMeta),
	}

	c.server()
	//TODO:check out timeout
	return &c
}
