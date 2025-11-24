package distributed

import "time"

// LocalWorker executa tasks de forma síncrona aplicando uma função injetada.
type LocalWorker struct {
	id       string
	handler  func(TaskRequest) TaskResult
	lastBeat time.Time
}

func NewLocalWorker(id string, handler func(TaskRequest) TaskResult) *LocalWorker {
	return &LocalWorker{
		id:       id,
		handler:  handler,
		lastBeat: time.Now(),
	}
}

func (w *LocalWorker) ID() string {
	return w.id
}

func (w *LocalWorker) Heartbeat() time.Time {
	w.lastBeat = time.Now()
	return w.lastBeat
}

func (w *LocalWorker) Execute(task TaskRequest) TaskResult {
	if w.handler == nil {
		return TaskResult{
			TaskID:   task.TaskID,
			WorkerID: w.id,
			Error:    "handler não definido",
		}
	}
	result := w.handler(task)
	result.WorkerID = w.id
	result.TaskID = task.TaskID
	return result
}
