package base

import (
	"context"
	"fmt"
	"sync"

	"go-executors/pkg/common"
	"go-executors/pkg/executor/core"
	"go.uber.org/zap"
)

type TaskActor struct {
	id int

	taskQueue chan core.Task
	queueSize int

	resultQueue chan core.Task

	closeChan chan bool

	mu            sync.Mutex
	alreadyClosed bool

	wg  *sync.WaitGroup
	ctx context.Context
	log *zap.SugaredLogger
}

func CreateTaskActor(ctx context.Context, id, internalQueueSize int, wg *sync.WaitGroup, resultQueue chan core.Task) TaskActor {
	log := common.LogFromContext(ctx)
	actor := TaskActor{
		id:          id,
		taskQueue:   make(chan core.Task, internalQueueSize),
		queueSize:   internalQueueSize,
		wg:          wg,
		ctx:         ctx,
		log:         log,
		resultQueue: resultQueue,
		//important to use buffered chanel
		closeChan: make(chan bool, 10),
	}

	log.Debugw("actor was created",
		"actor_id", id)

	go actor.Start()
	return actor
}

func (me *TaskActor) AddTask(task core.Task) (bool, error) {
	defer me.mu.Unlock()
	me.mu.Lock()

	switch {
	case me.alreadyClosed:
		me.log.Warnw("actor task queue closed, skip task",
			"actor_id", me.id,
			"task_id", task.Id())
		return false, common.ActorClosedError{Err: fmt.Errorf("actor [%v] closed", me.id)}
	case len(me.taskQueue) >= me.queueSize:
		//me.log.Debugw("actor queue is full, skip task",
		//	"actor_id", me.id,
		//	"task_id", task.Id())
		return false, nil
	}

	me.taskQueue <- task
	me.log.Debugw("task was added to actor queue",
		"actor_id", me.id,
		"task_id", task.Id())
	return true, nil
}

func (me *TaskActor) Start() {
	defer me.wg.Done()

	for {
		select {
		case <-me.ctx.Done():
			if err := me.ctx.Err(); err != nil {

				me.log.Warnw("shutdown actor with err",
					"actor_id", me.id,
					"err", err)
				return
			}

			me.log.Warnw("shutdown actor",
				"actor_id", me.id)
			return

		case currTask, stillOpen := <-me.taskQueue:
			if stillOpen == false {
				me.log.Infow("actor stopped", "actor_id", me.id)
				me.closeChan <- true
				return
			}

			result := currTask.Execute(me.ctx)
			me.resultQueue <- result

			me.log.Debugw("task was executed",
				"actor_id", me.id,
				"task_id", currTask.Id())
		}
	}
}

func (me *TaskActor) Stop() {
	defer me.mu.Unlock()
	me.mu.Lock()

	me.log.Infow("stop actor",
		"actor_id", me.id,
		"already_closed", me.alreadyClosed)

	if me.alreadyClosed {
		return
	}

	me.alreadyClosed = true
	close(me.taskQueue)
}

func (me *TaskActor) SyncStop() {
	me.Stop()
	<-me.closeChan
}
