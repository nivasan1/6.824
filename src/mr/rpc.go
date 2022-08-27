package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Response struct {
	fname string
	jobStatus int
	taskNum int
}

type Request struct {
	curJobFname string
	curJobStatus int
	resFname string
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
