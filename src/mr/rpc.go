package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

// Arguments for requesting a task from the coordinator.
type TaskRequest struct {
	WorkerID int
}

// Response with task details.
type TaskResponse struct {
	TaskType string // "map" or "reduce" or "wait" or "exit"
	TaskID   int
	FileName string // Only for map tasks
	NReduce  int
	NMap     int
}

// Arguments for reporting task completion.
type ReportRequest struct {
	WorkerID int
	TaskType string // "map" or "reduce"
	TaskID   int
}

// Response to the report request.
type ReportResponse struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type HeartbeatResponse struct {
}
