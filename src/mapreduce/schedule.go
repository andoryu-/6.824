package mapreduce

import "fmt"
import "time"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
// All ntasks tasks have to be scheduled on workers. Once all tasks
// have completed successfully, schedule() should return.
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
    var files_map *[]string

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
        files_map = &mapFiles
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
        alt := make([]string, ntasks)
        files_map = &alt
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

    var workers Clients = make([]Client, 0, 1)
    args := make(chan DoTaskArgs)
    cntl := make(chan int)
    go workers.Poller(registerChan, args, cntl)
    for taskIdx := 0; taskIdx < ntasks; taskIdx++ {
        args <- DoTaskArgs{jobName, (*files_map)[taskIdx], phase, taskIdx, n_other}
    }
    cntl <- 0
    <-cntl
	fmt.Printf("Schedule: %v done\n", phase)
}

type Client struct {
    Addr string
    Args chan DoTaskArgs
    Done chan int
}
type Clients []Client

func (self *Client) Run() {
    var count int
    for arg := range self.Args {
        call(self.Addr, "Worker.DoTask", arg, nil)
        count++
    }
    self.Done <- count
}

func (self *Clients) Poller(incoming chan string, args chan DoTaskArgs, cntl chan int) {
    done := make(chan int)
    for finish := false; !finish; {
        select {
            case <-cntl:
                finish = true
            case addr := <-incoming:
                *self = append(*self, Client{addr, args, done})
                go (*self)[len(*self) - 1].Run()
            case <-time.After(1 * time.Second):
                debug("Polling #worker %d\n", len(*self))
            default:
        }
    }
    close(args)
    var ntasks int
    for completion := 0; completion < len(*self); completion++ {
        i := <-done
        ntasks += i
    }
    cntl <- ntasks
    return
}

