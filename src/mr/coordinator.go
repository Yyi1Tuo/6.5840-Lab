package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"
import "sync"

const (
	MapPhase = 0
	ReducePhase = 1
	WaitPhase = 2
	MapTask = 0
	ReduceTask = 1
)//宏定义
type Task struct {
	WorkType int // 0: map, 1: reduce
	Filename string // 文件名
	TaskId int // 任务id，用于生成中间文件名mr-X-Y
	ReduceNum int //reduce任务数量
}

var mu sync.Mutex//互斥访问TaskMap
var PhaseMu sync.Mutex//互斥访问Phase

type Coordinator struct {
	// Your definitions here.
	//workers []*Worker
	TaskMap map[int] int //任务状态 0:map, 1:reduce, 2:wait map[taskId] status
	files []string
	Phase int // 0:map, 1:reduce, 2:done
	ReduceNum int //reduce任务数量
	MapTaskChan chan *Task //map任务管道
	ReduceTaskChan chan *Task //reduce任务管道
}

// Your code here -- RPC handlers for the worker to call.
const waitTime = 10 * time.Second //等待worker完成任务的时间，超出则认为worker挂了
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
	PhaseMu.Lock()
	defer PhaseMu.Unlock()
	if c.Phase == MapPhase {
		reply.Task = <-c.MapTaskChan
	} else if c.Phase == ReducePhase {
		reply.Task = <-c.ReduceTaskChan
	}
	return nil
}

func (c *Coordinator) CheakPhase(args *CheakPhaseArgs, reply *CheakPhaseReply) error {
	reply.Phase = c.Phase
	return nil
}

func (c *Coordinator) DoneReport(args *DoneReportArgs, reply *DoneReportReply) error {
	mu.Lock()
	defer mu.Unlock()
	//fmt.Println(c.TaskMap[args.Task])
	c.TaskMap[args.TaskId] = WaitPhase
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
	ret := false
	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskMap: make(map[int] int),
		files: files,
		ReduceNum: nReduce,
		MapTaskChan: make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		Phase: MapPhase,
	}
	c.server()
	// Your code here.
	MakeMapTask(files, &c)
	
	go func(){	
		for{
			mu.Lock()
			flag := 0 //free
			for _, status := range c.TaskMap {
				if status == MapPhase {
					flag = 1//busy
					break;
				}
			}
			mu.Unlock()
			if flag == 0 {
				fmt.Println("Map任务完成")
				PhaseMu.Lock()
				defer PhaseMu.Unlock()
				c.Phase = ReducePhase
				break;
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}()
	
	
	return &c
}
func MakeMapTask(files []string, c *Coordinator) {
	
	//
	// 生成map任务并写入管道
	//
	
	for i, file := range files {
		task := Task{
			WorkType: MapTask,
			Filename: file,
			TaskId: i,
			ReduceNum: c.ReduceNum,
		}
		c.MapTaskChan <- &task
		c.TaskMap[i] = MapPhase
	}
	fmt.Println("MapTask生成完成")
}
