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

var taskIdGen = IncreasingIdGen{
	seed: 0,
}

//------------------------------------->

//task,map or reduce task
//<-------------------------------------

type TaskMeta struct {
	WorkerId  *WorkerId
	StartTime *time.Time
	EndTime   *time.Time
	TaskRef   *Task
}

//------------------------------------->

type Phase int

const (
	InitialPhase Phase = iota
	MapPhase
	ReducePhase
	CompletedPhase
)

type Coordinator struct {
	InputFiles    []string
	NReduce       int
	taskQueue     chan *Task
	excecutePhase Phase
	taskMetaMap   map[TaskId]*TaskMeta
	idGenerator   IdGenerator
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(taskReq *TaskReqArg, taskResp *TaskReqReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.taskQueue) == 0 {
		//not task to do at that moment
		return nil
	}
	task := *<-c.taskQueue
	c.updateTaskMeta(taskReq.WorkerId, task)
	taskResp.Task = task
	taskResp.RespId = taskReq.ReqId
	//follow each assgined task
	go c.scanTaskStat(task.Id)
	return nil
}

func (c *Coordinator) HandleTaskReport(req *TaskReportArg, resp *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := req.Task
	c.updateTaskMeta(req.WorkerId, task)
	resp.RespId = req.ReqId
	return nil
}

func (c *Coordinator) updateTaskMeta(workerId WorkerId, task Task) {
	meta := c.taskMetaMap[task.Id]
	taskRet := task.Ret

	if orgTaskRet := meta.TaskRef.Ret; orgTaskRet != nil && *orgTaskRet == Success {
		//task has successfully finished
		return
	}
	now := time.Now()
	if taskRet == nil {
		meta.StartTime = &now
		meta.WorkerId = &workerId
	} else {
		meta.EndTime = &now
		meta.TaskRef.State = task.State
		meta.TaskRef.Ret = task.Ret
	}
	c.taskMetaMap[task.Id] = meta
}

var mu sync.Mutex
func (c *Coordinator) scanTaskStat(taskId TaskId) {
	mu.Lock()
	defer mu.Unlock()
	for {
		time.Sleep(time.Second * 10)
		meta := c.taskMetaMap[taskId]
		task := meta.TaskRef
		if task.State == InProgress {
			//reset task stat
			task.State = Idle
			meta.WorkerId = nil
			c.taskMetaMap[taskId] = meta
		}
	}
}

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
	return c.excecutePhase == CompletedPhase
}

func (c *Coordinator) initTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.excecutePhase != InitialPhase {
		return
	}
	for _, file := range c.InputFiles {
		task := Task{
			Id:        TaskId(c.idGenerator.GenerateId()),
			Type:      Map,
			State:     Idle,
			InputFile: file,
		}
		c.taskQueue <- &task
		meta := TaskMeta{
			TaskRef: &task,
		}
		c.taskMetaMap[task.Id] = &meta
	}
	c.excecutePhase = MapPhase
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
		taskQueue:     make(chan *Task, max(len(files), nReduce)),
		excecutePhase: MapPhase,
		taskMetaMap:   make(map[TaskId]*TaskMeta),
		idGenerator: &IncreasingIdGen{
			seed: 0,
		},
	}

	c.initTasks()
	c.server()
	//TODO:check out timeout
	//TODO:heart beat
	return &c
}
