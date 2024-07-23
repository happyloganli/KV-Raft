package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	RequestId string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
	Ok    bool
}

type GetArgs struct {
	Key       string
	RequestId string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
	Ok    bool
}

type AckArgs struct {
	RequestId string
	// You'll have to add definitions here.
}

type AckReply struct {
	Ok bool
}
