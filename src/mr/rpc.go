package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MsgId int

var reqIdGen = IncreasingIdGen{
	seed: 0,
}

//------------------------------------->
//heart beat

//<-------------------------------------

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskReqArgs struct {
	WorkerId WorkerId
	ReqId    MsgId
}

func MakeTaskArg(workerId WorkerId) TaskReqArgs {
	return TaskReqArgs{
		WorkerId: workerId,
		ReqId:    MsgId(reqIdGen.GenerateId()),
	}
}

type TaskReqReply struct {
	Task           Task
	jobFinishedSig bool
	RespId         MsgId
}

type TaskReportArgs struct {
	WorkerId
	ReqId MsgId
	Task
	RetPaths []string
}

func MakeReportArgs(workerId WorkerId, task *Task, retPaths []string) TaskReportArgs {
	return TaskReportArgs{
		WorkerId: workerId,
		ReqId:    MsgId(reqIdGen.GenerateId()),
		Task:     *task,
		RetPaths: retPaths,
	}
}

type TaskReportReply struct {
	RespId  MsgId
	retPath []string
}

type WorkerRegArgs struct {
	ReqId MsgId
}

func MakeWorkerRegArgs() WorkerRegArgs {
	return WorkerRegArgs{
		ReqId: MsgId(reqIdGen.GenerateId()),
	}
}

type WorkerRegReply struct {
	RespId MsgId
	WorkerId
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
