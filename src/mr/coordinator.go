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
	DeadPhase = 3
	DonePhase = 4
	MapTask = 0
	ReduceTask = 1
	
)//宏定义
type Task struct {
	WorkType int // 0: map, 1: reduce, 2: wait, 3:dead
	Filename string // 文件名
	TaskId int // 任务id，用于生成中间文件名mr-X-Y
	ReduceNum int //reduce任务数量
}

var mu sync.Mutex//互斥访问TaskMap
var TaskMu sync.Mutex//互斥访问Tasks
var PhaseMu sync.Mutex//互斥访问Phase
var HeartbeatMu sync.Mutex//互斥访问Heartbeat

type Coordinator struct {
	// Your definitions here.
	//workers []*Worker
	Tasks map[int] Task //任务 map[taskId] task
	TaskMap map[int] int //任务状态 0:map, 1:reduce, 2:wait,3:dead map[taskId] status
	HeartbeatMap map[int] time.Time//心跳时间 map[taskId]time
	files []string
	Phase int // 0:map, 1:reduce, 2:done
	ReduceNum int //reduce任务数量
	MapTaskChan chan *Task //map任务管道
	ReduceTaskChan chan *Task //reduce任务管道
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

func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
	PhaseMu.Lock()
	defer PhaseMu.Unlock()
	//设置一个超时器，如果阻塞过久，则认为没有任务了，直接返回
	if c.Phase == MapPhase {
		select {
		// 重要：先注册心跳，再分配任务
		case reply.Task = <-c.MapTaskChan:
			reply.Lenfiles = -1
			HeartbeatMu.Lock()
			c.HeartbeatMap[reply.Task.TaskId] = time.Now()
			HeartbeatMu.Unlock()
		case <-time.After(1500 * time.Millisecond):
			reply.Task = nil
			reply.Lenfiles = 0
		}
	} else if c.Phase == ReducePhase {
		select {
		case reply.Task = <-c.ReduceTaskChan:
			reply.Lenfiles = len(c.files)
			HeartbeatMu.Lock()
			c.HeartbeatMap[reply.Task.TaskId] = time.Now()
			HeartbeatMu.Unlock()
		case <-time.After(1500 * time.Millisecond):
			reply.Task = nil
			reply.Lenfiles = 0
		}
	}
	return nil
}

func (c *Coordinator) CheckPhase(args *CheckPhaseArgs, reply *CheckPhaseReply) error {
	PhaseMu.Lock()
	defer PhaseMu.Unlock()
	reply.Phase = c.Phase
	return nil
}

func (c *Coordinator) DoneReport(args *DoneReportArgs, reply *DoneReportReply) error {
	mu.Lock()
	defer mu.Unlock()
	HeartbeatMu.Lock()
	defer HeartbeatMu.Unlock()
	TaskMu.Lock()	
	defer TaskMu.Unlock()
	//fmt.Println(c.TaskMap[args.Task])
	delete(c.TaskMap, args.TaskId)
	delete(c.HeartbeatMap, args.TaskId)
	delete(c.Tasks, args.TaskId)
	//fmt.Println("Task",args.TaskId,"done")
	return nil
}
func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	HeartbeatMu.Lock()
	defer HeartbeatMu.Unlock()
	c.HeartbeatMap[args.Task.TaskId] = time.Now()
	//fmt.Println("Heartbeat received from task",args.Task.TaskId,"at",time.Now())
	//如果rpc调用失败，则返回false
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
	if c.Phase == DonePhase {
		ret = true
	}
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
		Tasks: make(map[int] Task),
		TaskMap: make(map[int] int),
		HeartbeatMap: make(map[int] time.Time),
		files: files,
		ReduceNum: nReduce,
		MapTaskChan: make(chan *Task, 2*len(files)),
		ReduceTaskChan: make(chan *Task, 2*nReduce),//多一倍缓冲区，防止阻塞
		Phase: MapPhase,
	}
	c.server()
	// Your code here.
	MakeMapTask(files, &c)
	go ReceiveHeartbeat(&c)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(){	
		defer wg.Done()
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
				PhaseMu.Lock()
				defer PhaseMu.Unlock()
				c.Phase = ReducePhase
				//fmt.Println("Map任务完成 State changed")
				break;
			}
			//fmt.Println("Map任务未完成")
			time.Sleep(100 * time.Millisecond)
		}
		
	}()

	wg.Wait()
	//reduce任务
	MakeReduceTask(&c)
	//等待reduce任务完成
	wg.Add(1)
	go func(){
		defer wg.Done()
		for{
			mu.Lock()
			flag := 0 //free
			for _, status := range c.TaskMap {
				if status == ReducePhase {
					flag = 1//busy
					break;
				}
			}
			mu.Unlock()
			if flag == 0 {
				PhaseMu.Lock()
				defer PhaseMu.Unlock()
				c.Phase = DonePhase
				//fmt.Println("Reduce任务完成 State changed")
				break;
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	wg.Wait()

	//所有任务完成 
	//fmt.Println("所有任务完成")
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
		//错误的做法在这里：c.HeartbeatMap[i] = time.Now()
		//原因在于假如迟迟没有worker来领取任务，则会源源不断的挂任务
		//正确的做法是：在任务被领取时，再去注册心跳时间
		c.Tasks[i] = task
	}
	//fmt.Println("MapTask生成完成")
}
func MakeReduceTask(c *Coordinator){
	//生成reduce任务并写入管道
	for i := 0; i< c.ReduceNum; i++ {
		task := Task{
			WorkType: ReduceTask,
			TaskId:   i,
			Filename: "",
			ReduceNum: c.ReduceNum,
		}
		c.ReduceTaskChan <- &task
		c.TaskMap[i] = ReducePhase
		c.Tasks[i] = task
	}
	//fmt.Println("ReduceTask生成完成")

}

func ReceiveHeartbeat(c *Coordinator) {
	//检查worker是否挂了
	//如果挂了，则重新分配任务
	ticker := time.NewTicker(5 * time.Second)
	for _ = range ticker.C {
		//每5秒检查一次心跳状态
		HeartbeatMu.Lock()
		//fmt.Println("HeartbeatMap:",c.HeartbeatMap)
		for taskId, lastTime := range c.HeartbeatMap {
			if time.Now().Sub(lastTime) > 5*time.Second {
			//如果心跳时间超过5秒，则认为任务挂了
				//如果任务挂了，则重新分配任务
				TaskMu.Lock()
				PhaseMu.Lock()
				fmt.Println("Task",taskId,"挂了")
				//找出此Taskid对应的任务
				var newtask Task
				newtask = c.Tasks[taskId]
				if c.Phase == MapPhase {
					// 重新将map任务放回channel
					c.MapTaskChan <- &newtask
				}else if c.Phase == ReducePhase {
					// 重新将reduce任务放回channel 
					c.ReduceTaskChan <- &newtask
				}	
				//更新心跳状态
				c.HeartbeatMap[taskId] = time.Now()	
				TaskMu.Unlock()
				PhaseMu.Unlock()
				}
				
			}
		HeartbeatMu.Unlock()
	}
		
}