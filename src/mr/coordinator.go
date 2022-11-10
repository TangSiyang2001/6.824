package mr

import (
	"errors"
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

type TaskId int

//map or reduce task
type Task struct {
	Id            TaskId
	Type          TaskType
	State         TaskState
	InputFileName []string
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

type WorkerId int

type Coordinator struct {
	InputFiles    []string
	NReduce       int
	taskQueue     chan *Task
	excecutePhase Phase
	taskMetaMap   map[TaskId]*TaskMeta
	intermediates [][]string
	NFinished     int
	taskIdGen     IdGenerator
	workerIdGen   IdGenerator
	phaseListener ChanListener
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleWorkerReg(regReq *WorkerRegArgs, regResp *WorkerRegReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if regReq == nil || regResp == nil {
		return errors.New("req or resp ptr should be non nil")
	}
	regResp.RespId = regReq.ReqId
	workerId := c.workerIdGen.GenerateId()
	regResp.WorkerId = WorkerId(workerId)
	regResp.NReduce = c.NReduce
	return nil
}

func (c *Coordinator) HandleTaskAssginment(taskReq *TaskReqArgs, taskResp *TaskReqReply) error {
	c.mu.Lock()
	if taskReq == nil || taskResp == nil {
		return errors.New("req or resp ptr should be non nil")
	}
	taskResp.RespId = taskReq.ReqId
	if c.excecutePhase == CompletedPhase {
		taskResp.jobFinishedSig = true
		return nil
	}
	//will blok until has tasks to dispatch
	task := *<-c.taskQueue
	c.updateTaskMeta(taskReq.WorkerId, task)
	taskResp.jobFinishedSig = false
	taskResp.Task = task
	//follow each assgined task,if timeout,assgin to another worker
	//The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time
	//(for this lab, use ten seconds)
	c.mu.Unlock()
	go c.scanTaskStat(task.Id)
	return nil
}

func (c *Coordinator) HandleTaskReport(req *TaskReportArgs, resp *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := req.Task
	//TODO:check if the map phase has finished
	c.updateTaskMeta(req.WorkerId, task)
	c.handleRetpaths(&req.RetPaths)
	c.checkPhase()
	resp.RespId = req.ReqId
	return nil
}

func (c *Coordinator) handleRetpaths(paths *[]string) {
	if c.excecutePhase == MapPhase {
		for i := 0; i < c.NReduce; i++ {
			c.intermediates[i] = append(c.intermediates[i], (*paths)[i])
		}
	}
}

func (c *Coordinator) checkPhase() {
	if (c.excecutePhase == MapPhase && c.NFinished == len(c.InputFiles)) || (c.excecutePhase == ReducePhase && c.NFinished == len(c.InputFiles)+c.NReduce) {
		c.phaseListener.Publish()
	}
}

func (c *Coordinator) updateTaskMeta(workerId WorkerId, task Task) {
	//TODO: reform:single responsibility
	meta := c.taskMetaMap[task.Id]
	if meta.TaskRef.State == Completed {
		//task has successfully finished
		return
	}
	stat := task.State
	now := time.Now()
	if stat == Idle {
		//initial update
		meta.StartTime = &now
		meta.WorkerId = &workerId
	} else if stat == Completed {
		//finishing update
		meta.EndTime = &now
		meta.TaskRef.State = task.State
		c.NFinished++
	}
	c.taskMetaMap[task.Id] = meta
}

func (c *Coordinator) scanTaskStat(taskId TaskId) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	finished := c.excecutePhase == CompletedPhase
	if finished {
		//wait for workers to quit
		time.Sleep(time.Second * 1)
	}
	return finished
}

func (c *Coordinator) initMapTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.excecutePhase != InitialPhase {
		return
	}
	c.excecutePhase = MapPhase
	for _, file := range c.InputFiles {
		task := Task{
			Id:            TaskId(c.taskIdGen.GenerateId()),
			Type:          Map,
			State:         Idle,
			InputFileName: []string{file},
		}
		c.publishTask(&task)
	}
}

func (c *Coordinator) initReduceTasks() {
	//TODO:implement
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.excecutePhase != MapPhase {
		return
	}
	c.excecutePhase = ReducePhase
	for _, intermediate := range c.intermediates {
		task := Task{
			Id:            TaskId(c.taskIdGen.GenerateId()),
			Type:          Reduce,
			State:         Idle,
			InputFileName: intermediate,
		}
		c.publishTask(&task)
	}
}

func (c *Coordinator) publishTask(task *Task) {
	c.taskQueue <- task
	meta := TaskMeta{
		TaskRef: task,
	}
	c.taskMetaMap[task.Id] = &meta
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
		excecutePhase: InitialPhase,
		taskMetaMap:   make(map[TaskId]*TaskMeta),
		taskIdGen: &IncreasingIdGen{
			seed: 0,
		},
		workerIdGen: &IncreasingIdGen{
			seed: 0,
		},
		phaseListener: MakeChanListener(),
		NFinished:     0,
		intermediates: make([][]string, nReduce),
	}
	//life cycle
	go func(c *Coordinator) {
		c.initMapTasks()
		c.phaseListener.Subscribe()
		c.initReduceTasks()
		c.phaseListener.Subscribe()
		c.excecutePhase = CompletedPhase
	}(&c)

	c.server()
	return &c
}
