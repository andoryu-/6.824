package mapreduce

import (
        "fmt"
        "os"
        "time"
       )

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

    // start workers
    var workers Clients = make([]Client, 0, 1)
    choice := make(chan int)
    stop := make(chan bool)
    go workers.Poller(registerChan, choice, stop)
    <-choice
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
    length := len(workers)
    if ntasks < length {
        length = ntasks
    }
    method := string("Worker.DoTask")
    taskIdx := int(0)
    var args DoTaskArgs
    args.JobName = jobName
    args.Phase = phase
    args.NumOtherPhase = n_other
    for i := 0; i < length; i++ {
        args.TaskNumber = taskIdx
        taskIdx++
        if phase == mapPhase {
            args.File = mapFiles[i]
        } else {
            args.File = ""
        }
        // submit to workers inital requests
        go workers[i].Submit(method, &args)
    }
    completion := int(0)
    for i := length; i < ntasks; i++ {
        args.TaskNumber = taskIdx
        taskIdx++
        if phase == mapPhase {
            args.File = mapFiles[i]
        } else {
            args.File = ""
        }
        // more requests for finished workers, also accept incoming workers
        x := <-choice
        go workers[x].Submit(method, &args)
        completion++
    }
    for ;completion < ntasks; completion++ {
        <-choice
    }
    stop <- true
	fmt.Printf("Schedule: %v done\n", phase)
}

type Client struct {
    Addr string
    Done chan int
}
type Clients []Client

func (self *Client) Submit(method string, args *DoTaskArgs) {
    call(self.Addr, method, *args, nil)
    self.Done <- 1
}

func (self *Clients) Poller(incoming chan string, rs chan int, stop chan bool) {
    for waited := false; len(*self) == 0 || waited == false; {
        select {
        case <-stop:
            return
        case addr := <-incoming:
            *self = append(*self, Client{addr, make(chan int)})
        case <-time.After(1 * time.Second):
            fmt.Fprintf(os.Stderr, "1s elapsed, len %d\n", len(*self))
            waited = true
        }
    }
    // the first rs here is to notify the completion of the preparation phase
    rs <- -1
    for {
        select {
        case <-stop:
            return
        case addr := <-incoming:
            *self = append(*self, Client{addr, make(chan int)})
        default:
        }
        for i := 0; i < len(*self); i++ {
            select {
            case <-(*self)[i].Done:
                rs <- i
            default:
            }
        }
    }
}

