package kvsrv

type ReportDoneArgs struct {
	Seq int64
}	
type ReportDoneReply struct {
	Seq int64
}
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq int64//序列号
}

type PutAppendReply struct {
	Value string
	Seq int64//序列号
}

type GetArgs struct {
	Key string
	Seq int64//序列号
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
	Seq int64//序列号
}
