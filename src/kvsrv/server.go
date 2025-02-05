package kvsrv

import (
	"log"
	"sync"
)

const Debug = false
const (
	RequestPhase=0
	ReplyPhase=1
)
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	kv map[string]string
	// Your definitions here.
	seqMap map[int]int //记录每个请求的执行状态，以便面对重复请求时，能够正确处理
	//问题在于这样的设计显然会导致最后服务器内存溢出
	//所以需要一个机制来清理已经执行过的请求
	MapCleaner chan int //用来清理seqMap的channel,为保证性能要带缓冲
}

func (kv *KVServer) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
	kv.MapCleaner<-args.Seq
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value=kv.kv[args.Key]
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.seqMap[args.Seq]==ReplyPhase{//如果seq已经执行过，直接返回
		reply.Value=kv.kv[args.Key]
		reply.Seq=args.Seq
		return
	}
	kv.kv[args.Key]=args.Value
	reply.Value=args.Value
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value=kv.kv[args.Key]
	kv.kv[args.Key]+=args.Value
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kv=make(map[string]string)
	kv.seqMap=make(map[int]int)
	kv.MapCleaner=make(chan int,10)
	// You may need initialization code here.
	go func(){
		for{
			ch:=<-kv.MapCleaner
			kv.mu.Lock()
			delete(kv.seqMap,ch)
			kv.mu.Unlock()
		}
	}()
	return kv
}
