package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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
func StartWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId, nReduce := RegWorker()
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
		var retPaths []string
		switch resp.Task.Type {
		case Map:
			retPaths, err = DoMap(mapf, &resp.Task, nReduce)
		case Reduce:
			err = DoReduce(reducef, &resp.Task)
		default:
			panic("unkonwn task")
		}
		if err != nil {
			log.Fatalf(err.Error())
		}
		resp.Task.State = Completed
		ReportTaskFinished(workerId, &resp.Task, retPaths)
	}

}

func RegWorker() (WorkerId, int) {
	req := MakeWorkerRegArgs()
	resp := WorkerRegReply{}
	ok := call("Coordinator.HandleWorkerReg", &req, &resp)
	handleRpcResp(req.ReqId, ok)
	return resp.WorkerId, resp.NReduce
}

func DoMap(mapf func(string, string) []KeyValue, task *Task, nReduce int) ([]string, error) {
	filename := task.InputFileName[0]
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()
	//execute map
	mapRet := mapf(filename, string(content))
	intermeidates := combile(&mapRet, nReduce)
	return spill(&intermeidates, task.Id, nReduce), nil
}

func combile(kvs *[]KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range *kvs {
		slot := ihash(kv.Key) % nReduce
		buckets[slot] = append(buckets[slot], kv)
	}
	return buckets
}

func spill(kvs *[][]KeyValue, taskId TaskId, nReduce int) []string {
	tmpPaths := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		kv := (*kvs)[i]
		path, _ := doSpill(&kv, int(taskId), i)
		tmpPaths = append(tmpPaths, path)
	}
	return tmpPaths
}

func doSpill(content *[]KeyValue, x int, y int) (string, error) {
	dir, _ := os.Getwd()
	//To ensure that nobody observes partially written files in the presence of crashes,
	//the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once
	//it is completely written. You can use ioutil.TempFile to create a temporary file and os.
	//Rename to atomically rename it.
	file, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
		return "", err
	}
	enc := json.NewEncoder(file)
	for _, kv := range *content {
		err := enc.Encode(kv)
		if err != nil {
			log.Fatalf("Failed to write k:%v,v:%v", kv.Key, kv.Value)
		}
	}
	file.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(file.Name(), outputName)
	return filepath.Join(dir, outputName), nil
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

func ReportTaskFinished(workerId WorkerId, task *Task, retPaths []string) {
	task.State = Completed
	req := MakeReportArgs(workerId, task, retPaths)
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
