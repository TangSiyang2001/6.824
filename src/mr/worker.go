package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
	workerId := RegWorker()
	for {
		retry := 0
		//will block if it needs to wait
		resp := ReqForTask(workerId)
		if resp == nil && retry < 3 {
			retry++
			time.Sleep(time.Microsecond * 100)
			continue
		}
		if retry == 3 || resp.jobFinishedSig {
			//retry for three times,if not work,worker quit
			break
		}

		var err error

		switch resp.Task.Type {
		case Map:
			err = DoMap(mapf, &resp.Task)
		case Reduce:
			err = DoReduce(reducef, &resp.Task)
		default:
			panic("unkonwn task")
		}
		if err != nil {
			log.Fatalf(err.Error())
		}
		ReportTaskFinished(workerId, &resp.Task)
	}

}

func RegWorker() WorkerId {
	req := MakeWorkerRegArgs()
	resp := WorkerRegReply{}
	ok := call("Coordinator.HandleWorkerReg", &req, &resp)
	handleRpcResp(req.ReqId, ok)
	return resp.WorkerId
}

func DoMap(mapf func(string, string) []KeyValue, task *Task) error {
	//TODO:implement
	return nil
}

func DoReduce(reducef func(string, []string) string, task *Task) error {
	//TODO:implement
	return nil
}

func ReqForTask(workerId WorkerId) *TaskReqReply {
	//async
	req := MakeTaskArg(workerId)
	resp := TaskReqReply{}
	ok := call("Coordinator.HandleTaskAssginment", &req, &resp)
	handleRpcResp(req.ReqId, ok)
	if ok {
		return &resp
	}
	return nil
}

func ReportTaskFinished(workerId WorkerId, task *Task) {
	task.State = Completed
	req := MakeReportArgs(workerId, task)
	resp := TaskReportReply{}
	ok := call("Coordinator.HandleTaskReport", &req, &resp)
	handleRpcResp(req.ReqId, ok)
}

func handleRpcResp(msgId MsgId, succ bool) {
	if succ {
		fmt.Printf("msg %v recieved.\n", msgId)
	} else {
		log.Fatalf("RPC failed ,msg %v.\n", msgId)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
