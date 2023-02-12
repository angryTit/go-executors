package base

import (
	"context"
	"fmt"
	"sync"

	"go-executors/pkg/common"
	"go-executors/pkg/executor/core"
	"go.uber.org/zap"
)

const infinityQueue = 10e6

type AsyncPoolActor struct {
	name     string
	actors   []core.Actor
	actorsWg *sync.WaitGroup

	taskQueue chan core.Task
	closeChan chan bool

	mu            sync.Mutex
	alreadyClosed bool
	wg            *sync.WaitGroup

	ctx context.Context
	log *zap.SugaredLogger
}

func CreateAsyncPoolActor(ctx context.Context, poolName string, poolSize, eachActorQueueSize int, resultQueue chan core.Task,
	poolWg *sync.WaitGroup) AsyncPoolActor {
	actors := make([]core.Actor, 0)
	actorsWg := &sync.WaitGroup{}
	for i := 0; i < poolSize; i++ {
		actorsWg.Add(1)
		eachActor := CreateTaskActor(ctx, i, eachActorQueueSize, actorsWg, resultQueue)
		actors = append(actors, &eachActor)
	}
	log := common.LogFromContext(ctx)
	log.Infow("pool was created",
		"name", poolName,
		"size", poolSize,
		"internal_actor_queue_size", eachActorQueueSize)

	pool := AsyncPoolActor{
		name:     poolName,
		actors:   actors,
		actorsWg: actorsWg,

		taskQueue: make(chan core.Task, infinityQueue),
		closeChan: make(chan bool, 10),
		wg:        poolWg,
		ctx:       ctx,
		log:       log,
	}

	go pool.Start()
	return pool
}

func (me *AsyncPoolActor) AddTask(task core.Task) (bool, error) {
	defer me.mu.Unlock()
	me.mu.Lock()

	if me.alreadyClosed {
		me.log.Warnw("pool task queue closed, skip task",
			"name", me.name,
			"task_id", task.Id())
		return false, common.ActorClosedError{Err: fmt.Errorf("pool [%v] closed", me.name)}
	}

	me.taskQueue <- task
	me.log.Debugw("task was added to pool queue",
		"name", me.name,
		"task_id", task.Id())
	return true, nil
}

func (me *AsyncPoolActor) Start() {
	defer me.wg.Done()

	for {
		select {
		case <-me.ctx.Done():
			if err := me.ctx.Err(); err != nil {

				me.log.Warnw("shutdown pool with err",
					"name", me.name,
					"err", err)
				return
			}

			me.log.Warnw("shutdown pool",
				"name", me.name)
			return

		case currTask, stillOpen := <-me.taskQueue:
			if stillOpen == false {
				me.log.Infow("pool stopped", "name", me.name)
				me.closeChan <- true
				return
			}

			for {
				l := len(me.actors)
				closed := 0
				success := false

				for _, eachActor := range me.actors {
					ok, err := eachActor.AddTask(currTask)
					switch {
					case err != nil:
						closed++
					case ok:
						success = true
					}

					if closed == l {
						me.log.Infow("pool closed", "name", me.name)
						me.closeChan <- true
						return
					}

					if success {
						me.log.Debugw("task added to actors queue by pool",
							"name", me.name,
							"task_id", currTask.Id())
						break
					}
				}

				if success {
					break
				}
			}

		} //end select
	} //end loop
}

func (me *AsyncPoolActor) Stop() {
	defer me.mu.Unlock()
	me.mu.Lock()

	me.log.Infow("stop pool",
		"name", me.name,
		"already_closed", me.alreadyClosed)

	if me.alreadyClosed {
		return
	}

	me.alreadyClosed = true
	close(me.taskQueue)

	for _, eachActor := range me.actors {
		eachActor.Stop()
	}
}

func (me *AsyncPoolActor) SyncStop() {
	me.Stop()
	<-me.closeChan
	me.actorsWg.Wait()
}
