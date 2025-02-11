package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

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
	Cache map[int64]string //用来缓存操作结果，同时替代了seqmap的作用

}

func (kv *KVServer) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Seq=args.Seq
	if _,ok:=kv.Cache[args.Seq];ok{
		delete(kv.Cache,args.Seq)
	}
	
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res,ok:=kv.Cache[args.Seq]
	if ok{
		reply.Value=res
		reply.Seq=args.Seq
		return
	}
	reply.Value=kv.kv[args.Key]
	kv.Cache[args.Seq]=reply.Value
	reply.Seq=args.Seq

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	res,ok:=kv.Cache[args.Seq]
	if ok{
		reply.Value=res
		reply.Seq=args.Seq
		return
	}
	oldvalue:=kv.kv[args.Key]
	kv.kv[args.Key]=args.Value
	kv.Cache[args.Seq]=oldvalue
	reply.Seq=args.Seq
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res,ok:=kv.Cache[args.Seq]
	if ok{
		reply.Value=res
		reply.Seq=args.Seq
		return
	}
	//如果是新请求
	oldvalue:=kv.kv[args.Key]
	reply.Value=oldvalue
	kv.Cache[args.Seq]=oldvalue
	kv.kv[args.Key]+=args.Value
	reply.Seq=args.Seq
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kv = make(map[string]string)
	kv.Cache = make(map[int64]string)
	// You may need initialization code here.
	return kv
}
