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
import "time"

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
	for {
		phase := CheckPhase()
		//这里可能导致死锁，假如worker拿到了phase=map进入gettask后，事实上这个时候task已经分配完毕了，
		//而coordinator的phase变为reduce，worker会一直等待，直到timeout

		//所以allocateTask需要返回一个bool，表示是否还有任务
		//如果还有任务，则继续执行，否则退出
		switch phase {
		case MapPhase:
			taskPtr,_ := GetTask()
			if taskPtr == nil {
				continue
			}
			DoMapTask(taskPtr, mapf)
		case ReducePhase:
			taskPtr,lenfiles := GetTask()
			if taskPtr == nil {
				continue
			}
			DoReduceTask(taskPtr, reducef,lenfiles)
		case WaitPhase:
			time.Sleep(1000 * time.Millisecond)
		case DonePhase:
			return
		}
		time.Sleep(1000 * time.Millisecond)
		//fmt.Println("Current Phase : ",phase)
	}

	
}
func DoneReport(taskPtr *Task) {
	args := DoneReportArgs{TaskId: taskPtr.TaskId}
	reply := DoneReportReply{}
	call("Coordinator.DoneReport", &args, &reply)
}
func CheckPhase() int {	
	args := CheckPhaseArgs{}
	reply := CheckPhaseReply{}
	call("Coordinator.CheckPhase", &args, &reply)
	return reply.Phase
}
func GetTask() (*Task,int) {
	// 从coordinator获取任务
	args := AllocateTaskArgs{}
	reply := AllocateTaskReply{}
    call("Coordinator.AllocateTask", &args, &reply)
	return reply.Task,reply.Lenfiles
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	// 执行任务
	done := make(chan bool)
	go SendHeartbeat(task,done)
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
	done <- true
	DoneReport(task)
}

func DoReduceTask(task *Task, reducef func(string, []string) string,lenfiles int) {
	// 执行任务
	done := make(chan bool)
	go SendHeartbeat(task,done)//启动心跳

	// 读取中间文件
	intermediate := []KeyValue{}
	for i:=0;i<lenfiles;i++{
		oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskId)
		file, err := os.Open(oname)
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	// 调用reducef函数
	outname := "mr-out-" + strconv.Itoa(task.TaskId)
	outfile, _ := os.Create(outname)

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

		// 按照正确的格式写入每一行reduce输出
		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	outfile.Close()
	fmt.Println("File created")
	done <- true
	DoneReport(task)
}
func SendHeartbeat(task *Task,done chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	for _ = range ticker.C {
		args := HeartbeatArgs{Task: task}
		reply := HeartbeatReply{}
		call("Coordinator.Heartbeat", &args, &reply)
		if <-done{
			break
		}
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
