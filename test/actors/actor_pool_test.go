package actors

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go-executors/pkg/common"
	"go-executors/pkg/executor/base"
	"go-executors/pkg/executor/core"
)

type BasicTask struct {
	id       string
	taskType core.TaskType
}

func (b BasicTask) Id() string {
	return b.id
}

func (b BasicTask) Execute(ctx context.Context) core.Task {
	log := common.LogFromContext(ctx)

	time.Sleep(2 * time.Second)

	log.Debugf("FINISH EXECUTE TASK [%v] at [%v]", b.id, time.Now().UnixMilli())
	return BasicTask{id: "finish", taskType: core.NilTask}
}

func (b BasicTask) Type() core.TaskType {
	return b.taskType
}

func (b BasicTask) Err() error {
	return nil
}

func init() {
	common.LogInit("debug")
}

func TestBasic(t *testing.T) {
	ctx := context.Background()
	ctx, _ = common.WithLog(ctx)

	resultQueue := make(chan core.Task, 10)
	var poolWg sync.WaitGroup
	poolWg.Add(1)
	actorPool := base.CreateAsyncPoolActor(ctx, "basicPool", 2, 1, resultQueue, &poolWg)

	for i := 0; i < 6; i++ {
		task := BasicTask{id: fmt.Sprintf("task_%v", i), taskType: core.NilTask}
		actorPool.AddTask(task)
	}

	time.Sleep(8 * time.Second)
	actorPool.SyncStop()
}
