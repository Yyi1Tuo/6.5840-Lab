package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
    taskPtr := GetTask(0)
	if taskPtr.WorkType == 0 {
		DoMapTask(taskPtr, mapf)
	} else {
		DoReduceTask(taskPtr, reducef)
	}
	//fmt.Println(*taskPtr)
	// uncomment to send the Example RPC to the coordinator.
	 //CallExample()
	
}

func GetTask(workType int) *Task {
	// 从coordinator获取任务
	args := AllocateTaskArgs{workType}
	reply := AllocateTaskReply{}
    call("Coordinator.AllocateTask", &args, &reply)
	return reply.Task
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	// 执行任务
	intermediate := []KeyValue{} //中间结果
	file, err := os.Open(task.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}

	intermediate = mapf(task.Filename, string(content))
	sort.Sort(ByKey(intermediate))

	// 将map结果根据ihash分配到对应reduce任务
	HashKV := make([][]KeyValue, task.ReduceNum) //HashKV[reduce任务id] = []KeyValue
	for _, v := range intermediate {
		index := ihash(v.Key) % task.ReduceNum
		HashKV[index] = append(HashKV[index], v)
	}

	// 将reduce结果写入中间文件
	for i:=0;i<task.ReduceNum;i++{
		oname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, ok := os.Create(oname)
		if ok != nil {
			log.Fatalf("cannot create %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range HashKV[i] {
			err:=enc.Encode(kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		ofile.Close()
	}
	
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	// 执行任务
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
