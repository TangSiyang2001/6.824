package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//for sorting
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	wrr := RegWorker()
	if wrr == nil {
		log.Fatal("register worker failed")
		return
	}
	workerId := wrr.WorkerId
	nReduce := wrr.NReduce
	nMap := wrr.NMap
	retry := 0
	for {
		//will block if it needs to wait
		resp := ReqForTask(workerId)
		if resp == nil {
			break
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
			if retPaths == nil {
				return
			}
		case Reduce:
			retPath, e := DoReduce(reducef, &resp.Task, nMap)
			err = e
			retPaths = append(retPaths, retPath)
		default:
			panic("unkonwn task")
		}
		if err != nil {
			log.Fatalf(err.Error())
		}
		resp.Task.State = Completed
		ReportTaskFinished(workerId, &resp.Task, retPaths)
	}
	fmt.Printf("worker %d exit", workerId)
}

func RegWorker() *WorkerRegReply {
	req := MakeWorkerRegArgs()
	resp := &WorkerRegReply{}
	ok := call("Coordinator.HandleWorkerReg", &req, resp)
	handleRpcResp(req.ReqId, ok)
	if !ok {
		return nil
	}
	return resp
}

func DoMap(mapf func(string, string) []KeyValue, task *Task, nReduce int) ([]string, error) {
	if len(task.InputFiles) == 0 {
		return nil, nil
	}
	filename := task.InputFiles[0]
	content := ReadFile(filename)
	if content == nil {
		return nil, errors.New("read file" + filename + "failed")
	}
	//execute map
	mapRet := mapf(filename, string(content))
	intermeidates := combile(&mapRet, nReduce)
	return spill(intermeidates, task.Id, nReduce), nil
}

func combile(kvs *[]KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range *kvs {
		slot := ihash(kv.Key) % nReduce
		buckets[slot] = append(buckets[slot], kv)
	}
	return buckets
}

func spill(kvs [][]KeyValue, taskId TaskId, nReduce int) []string {
	tmpPaths := make([]string, 0)
	for i := 0; i < nReduce; i++ {
		kv := kvs[i]
		path := doSpill(kv, int(taskId), i)
		tmpPaths = append(tmpPaths, path)
	}
	return tmpPaths
}

func doSpill(content []KeyValue, x int, y int) string {
	//To ensure that nobody observes partially written files in the presence of crashes,
	//the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once
	//it is completely written. You can use ioutil.TempFile to create a temporary file and os.
	//Rename to atomically rename it.
	dir, _ := os.Getwd()
	file := CreateTmpFile(dir, "mr-tmp-*")
	enc := json.NewEncoder(file)
	for _, kv := range content {
		err := enc.Encode(kv)
		if err != nil {
			log.Fatalf("Failed to write k:%v,v:%v", kv.Key, kv.Value)
		}
	}
	file.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(file.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func DoReduce(reducef func(string, []string) string, task *Task, nMap int) (string, error) {
	reduceNum := int(task.Id) - nMap
	intermediate := readIntermedia(task.InputFiles)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	file := CreateTmpFile(dir, "mr-tmp-out-*")
	if file == nil {
		return "", errors.New("create temp file failed")
	}

	//from example,execute reducef in each distinct key
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	file.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceNum)
	os.Rename(file.Name(), oname)
	return oname, nil
}

func readIntermedia(paths []string) []KeyValue {
	ret := []KeyValue{}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			log.Fatal("Failed to open file "+path, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			ret = append(ret, kv)
		}
		file.Close()
	}
	return ret
}

func ReqForTask(workerId WorkerId) *TaskReqReply {
	//async
	req := MakeTaskArg(workerId)
	resp := &TaskReqReply{}
	ok := call("Coordinator.HandleTaskAssginment", &req, resp)
	handleRpcResp(req.ReqId, ok)
	if ok {
		return resp
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
		// fmt.Printf("msg %v recieved.\n", msgId)
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
