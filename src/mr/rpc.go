package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Response struct {
	Fname     string // fname to read and process from
	JobStatus int    // status of job
	TaskNum   int    //
}

type Request struct {
	CurJobFname  string
	CurJobStatus int // job status after response has been processed
}

func (r *Request) String() string {
	return fmt.Sprintf("%s:%d", r.CurJobFname, r.CurJobStatus)
}

func (r *Response) String() string {
	return fmt.Sprintf("%s, %d, %d", r.Fname, r.JobStatus, r.TaskNum)
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/nikhil-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
