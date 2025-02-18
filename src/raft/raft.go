package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//"fmt"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	leader = 2
	candidate = 1
	follower = 0
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	CommandIndex int
	Term int
}
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//持久化状态
	CurrentTerm int //当前任期
	VoteFor int //投票给谁
	Logs []LogEntry //日志
	
	//易失状态
	CommitIndex int //已提交的日志索引
	LastApplied int //已应用的日志索引
	NextIndex []int //下一个日志索引
	MatchIndex []int //匹配的日志索引
	
	//选举状态
	State int //选举状态
	Timer *time.Timer //选举&心跳计时器
	VoteCount int //收到的票数
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int //候选人任期
	CandidateId int //候选人ID
	LastLogIndex int //候选人最后日志索引
	LastLogTerm int //候选人最后日志任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int //当前任期
	VoteGranted bool //是否投票
}

// AppendEntriesArgs 是 AppendEntries RPC 的参数结构体
// 可以将其设置为null来作为heartbeat信号
type AppendEntriesArgs struct {
	Term int //当前任期
	LeaderId int //领导者ID
	PrevLogIndex int //前一个日志索引
	PrevLogTerm int //前一个日志任期
	Entries []LogEntry //日志条目
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//发送拉票请求
	for !rf.peers[server].Call("Raft.RequestVote", args, reply) {}
	
	if reply.VoteGranted {
		rf.mu.Lock()
		//fmt.Println(rf.me," get a Vote from ",server)
		rf.VoteCount++
		//fmt.Println(rf.me," VoteCount is ",rf.VoteCount)
		//如果收到的票数大于一半，则成为leader
		if rf.VoteCount > len(rf.peers)/2 {
			rf.State = leader
			rf.becomeLeader()
		}
		rf.mu.Unlock()
	}
	return reply.VoteGranted
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//接受拉票请求，这里的rf就是接受请求的server
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me," RequestVote from ",args.CandidateId)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	//如果请求的term小于当前任期，则不投票
	if args.Term < rf.CurrentTerm {
		return
	} 
	//如果candidate的term更大，可以投票
	if args.Term > rf.CurrentTerm{
		rf.VoteFor = args.CandidateId
		rf.State = follower
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		rf.Timer.Reset(time.Duration(rand.Intn(150)+250) * time.Millisecond)
		return
	}
	//如果candidate的term等于当前任期，假如没有投过票，则可以投票
	if args.Term == rf.CurrentTerm && rf.VoteFor == -1{
		rf.VoteFor = args.CandidateId
		reply.VoteGranted = true
		rf.Timer.Reset(time.Duration(rand.Intn(150)+250) * time.Millisecond)
		return
	}
	return

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.Timer.C:
			rf.mu.Lock()
			switch rf.State {
			case follower, candidate:
				// 作为 follower 或 candidate 超时后开始选举
				rf.startElection()
			case leader:
				// 作为 leader 发送心跳
				rf.broadcastHeartbeat()
				// leader 重置定时器为较短的心跳间隔
				rf.Timer.Reset(100 * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	//fmt.Println(rf.me," startElection")
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//fmt.Println(rf.me," startElection")
	switch rf.State {
	case follower:
		rf.State = candidate
		fallthrough
	case candidate:
		rf.CurrentTerm++
		rf.VoteFor = rf.me
		rf.Timer.Reset(time.Duration(rand.Intn(150)+250) * time.Millisecond)
		rf.VoteCount = 1
		//开始拉票
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			args := &RequestVoteArgs{Term:rf.CurrentTerm,CandidateId:rf.me,}//LastLogIndex:len(rf.Logs)-1,LastLogTerm:rf.Logs[len(rf.Logs)-1].Term}
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	//fmt.Println(rf.me," broadcastHeartbeat")
	if rf.State != leader {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		//由于传输的是心跳信号，部分内容可以设置为空
		args := &AppendEntriesArgs{Term:rf.CurrentTerm, LeaderId:rf.me,}
		reply := &AppendEntriesReply{}
		//发送心跳,注意脑裂问题
		go rf.sendAppendEntries(i, args, reply)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for !rf.peers[server].Call("Raft.AppendEntries", args, reply) {}

	//为防止脑裂，需要判断reply是否合法	
	if !reply.Success {
		rf.mu.Lock()
		rf.State = follower
		rf.CurrentTerm = reply.Term
		rf.VoteCount = 0
		rf.Timer.Reset(time.Duration(rand.Intn(150)+250) * time.Millisecond)
		rf.mu.Unlock()
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//接受到心跳信号
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.me," receive heartbeat from ",args.LeaderId)
	if args.Term < rf.CurrentTerm {//这里的rf是接受信号的server,如果leader的term更小说明发生脑裂了
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else {
		rf.CurrentTerm = args.Term
		rf.VoteCount = 0
		rf.Timer.Reset(time.Duration(rand.Intn(150)+250) * time.Millisecond)
		reply.Success = true
	}
}




// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Logs = []LogEntry{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.State = follower
	rf.Timer = time.NewTimer(rf.getRandomElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

// 获取随机选举超时时间
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(150) + 250) * time.Millisecond
}

// 状态转换时的定时器处理
func (rf *Raft) becomeFollower(term int) {
	rf.State = follower
	rf.CurrentTerm = term
	rf.VoteFor = -1
	// follower 使用随机的选举超时时间
	rf.Timer.Reset(rf.getRandomElectionTimeout())
}

func (rf *Raft) becomeLeader() {
	//fmt.Println(rf.me," becomeLeader")
	// leader 使用固定的心跳间隔
	rf.Timer.Reset(100 * time.Millisecond)
	// 初始化 leader 状态
	for i := range rf.peers {
		rf.NextIndex[i] = len(rf.Logs)
		rf.MatchIndex[i] = 0
	}
	// 立即发送第一次心跳
	rf.broadcastHeartbeat()
}
