package core

import (
	"context"
)

type Actor interface {
	Start()
	AddTask(task Task) (bool, error)
	Stop()
	SyncStop()
}

type TaskType string

const (
	NilTask TaskType = "NilTask"
)

type Task interface {
	Id() string
	Execute(ctx context.Context) Task
	Type() TaskType
	Err() error
}
