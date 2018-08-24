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
var indexMutex sync.Mutex

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
	var exitChan = make(chan int, 1)
	var worksChan = make(chan string, 100000)
	go func() {
		finish := false
		for {
			select {
			case worker := <-registerChan:
				worksChan <- worker
			case <-exitChan:
				finish = true

			}
			if finish {
				return
			}

		}

	}()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	m := make(map[int]int)
	args := DoTaskArgs{}
	args.JobName = jobName
	args.NumOtherPhase = n_other
	args.Phase = phase

	//fmt.Printf("ntask %d phone %v\n", ntasks, phase)
	for {
		var breakFlag bool

		worker := <-worksChan
		getIndex := getUnDoTaskIndex(m, ntasks)
		if getIndex == -1 {
			//fmt.Println("all done :", ntasks, phase)
			breakFlag = true
		} else if getIndex == -2 {
			//fmt.Println("all doing:", phase)
			worksChan <- worker
		} else {
			//fmt.Println("getindex:", getIndex)
			switch phase {
			case mapPhase:
				args.TaskNumber = getIndex
				args.File = mapFiles[getIndex]
				go func(num int) {
					flag := call(worker, "Worker.DoTask", &args, nil)
					if true != flag {
						setTaskIndex(m, num, statusUnDo)
						//worksChan <- worker

					} else {
						setTaskIndex(m, num, statusDone)
						worksChan <- worker
					}
				}(getIndex)
			case reducePhase:
				args.TaskNumber = getIndex
				go func(num int) {
					flag := call(worker, "Worker.DoTask", &args, nil)
					//fmt.Println("reduce flag:", flag)
					if true != flag {
						setTaskIndex(m, num, statusUnDo)
						//worksChan <- worker

					} else {
						setTaskIndex(m, num, statusDone)
						worksChan <- worker
					}
				}(getIndex)

			}
		}

		if breakFlag {
			break
		}

	}
	exitChan <- 1

	fmt.Printf("Schedule: %v done\n", phase)
}

const (
	statusUnDo  = 0
	statusDoing = 1
	statusDone  = 2
)

func getUnDoTaskIndex(m map[int]int, ntasks int) int {
	indexMutex.Lock()
	defer indexMutex.Unlock()
	var index = -1
	var status int
	var done int
	for i := 0; i < ntasks; i++ {
		status = m[i]
		if status == statusDone {
			done++
		}
		if status == statusUnDo {
			index = i
			m[i] = statusDoing
			break
		}
	}

	if done == ntasks {
		return -1
	}
	if index == -1 {

		return -2
	}
	return index

}
func setTaskIndex(m map[int]int, index int, status int) {
	indexMutex.Lock()
	m[index] = status
	indexMutex.Unlock()
}
