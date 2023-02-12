package common

type ActorClosedError struct {
	Err error
}

func (e ActorClosedError) Error() string {
	return e.Err.Error()
}
