package mapreduce

import (
	"fmt"
	"sync"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	idle_workers := make(chan string, ntasks)
	tasks := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		tasks <- i
	}

	go func() {
		for {
			new_worker := <-registerChan
			idle_workers <-new_worker
		}
	}()
	var (
		count      = 0
		task_lock sync.Mutex
	)
	for {
		worker := <-idle_workers
		i, ok := <-tasks
		if !ok {
			fmt.Printf("Finish: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
			return
		}
		go func() {
			success := call(worker, "Worker.DoTask",
				DoTaskArgs{jobName, mapFiles[i], phase, i, n_other},
				nil)
			idle_workers <-worker
			if (success) {
				fmt.Printf("finish task %v!\n", i)
				task_lock.Lock()
				count++
				if count == ntasks {
					close(tasks)
				} else {
					task_lock.Unlock()
				}
			} else {
				fmt.Printf("task %v failed!\n", i)
				task_lock.Lock()
				tasks <- i
				task_lock.Unlock()
			}

		}()
	}
}
