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
	Cache map[int]string //用来缓存append操作的值

}

func (kv *KVServer) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Seq=args.Seq
	if kv.seqMap[args.Seq]==ReplyPhase{
		delete(kv.Cache,args.Seq)
		delete(kv.seqMap,args.Seq)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//get操作理论上应该也需要缓存，假设第一次get返回失败，这时候别的client对value进行修改会导致
	//get返回的值不正确
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Println("Keys:", args.Key)
	//fmt.Println(kv.kv[args.Key])
	if kv.seqMap[args.Seq]==ReplyPhase {
		reply.Value=kv.Cache[args.Seq]
		reply.Seq=args.Seq
		return
	}
	reply.Value=kv.kv[args.Key]
	kv.Cache[args.Seq]=reply.Value
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.seqMap[args.Seq]==ReplyPhase{//如果seq已经执行过，直接返回
		reply.Value=""
		reply.Seq=args.Seq
		return
	}
	kv.kv[args.Key]=args.Value
	reply.Value=""
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.seqMap[args.Seq]==ReplyPhase{//如果seq已经执行过，直接返回
		//这里的问题在于，如果seq已经执行过，那么kv.kv[args.Key]的值已经变化了	
		//所以需要先保存原来的值，再返回
		reply.Value=kv.Cache[args.Seq]
		reply.Seq=args.Seq
		return
	}
	//如果是新请求
	oldvalue:=kv.kv[args.Key]

	reply.Value=oldvalue
	kv.Cache[args.Seq]=oldvalue
	kv.kv[args.Key]+=args.Value
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kv = make(map[string]string)
	kv.seqMap = make(map[int]int)
	kv.Cache = make(map[int]string)
	// You may need initialization code here.
	return kv
}
