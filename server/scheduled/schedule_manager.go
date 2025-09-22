package scheduled

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
)

// ScheduleManager manages scheduled jobs.
type ScheduleManager struct {
	scheduler gocron.Scheduler
	jobs      map[string]*JobDefinitionN
	mu        sync.Mutex
}

// JobDefinitionN defines a job to be scheduled.
// TODO: rename to JobDefinition, temporary name because of conflict
type JobDefinitionN struct {
	name string
	// very flexible, can be CronJob, DailyJob, DurationJob, DurationRandomJob, etc. Essentially anything supported by the gocron library
	// see https://pkg.go.dev/github.com/go-co-op/gocron/v2#example-CronJob for example
	job gocron.JobDefinition
	// the gocron.Task is essentially a wrapper around the actual function to be executed
	// see https://pkg.go.dev/github.com/go-co-op/gocron/v2#Task
	task gocron.Task
	// extra job options, for example:
	// https://pkg.go.dev/github.com/go-co-op/gocron/v2#example-WithStopTimeout
	options []gocron.JobOption
}

var (
	manager                     *ScheduleManager
	managerOnce                 sync.Once
	SchedulerTimeZone           = time.UTC // TODO: server flag?
	SchedulerConcurrentJobLimit = uint(5)  // TODO: server flag?
	ErrJobAlreadyExists         = fmt.Errorf("job already exists")
)

// GetManager returns the singleton SchedulerManager.
func GetManager() *ScheduleManager {
	managerOnce.Do(func() {
		s, _ := gocron.NewScheduler(
			gocron.WithLocation(SchedulerTimeZone),
			gocron.WithLimitConcurrentJobs(SchedulerConcurrentJobLimit, gocron.LimitModeWait),
		)
		manager = &ScheduleManager{
			scheduler: s,
			jobs:      make(map[string]*JobDefinitionN),
		}
	})
	return manager
}

// RegisterJob registers a job with a given name
func (m *ScheduleManager) RegisterJob(definition *JobDefinitionN) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.jobs[definition.name]; exists {
		return ErrJobAlreadyExists
	}
	_, err := m.scheduler.NewJob(definition.job, definition.task, definition.options...)
	if err != nil {
		return err
	}
	m.jobs[definition.name] = definition
	return nil
}

// HasJob returns true if a job with the given name has been registered.
func (m *ScheduleManager) HasJob(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.jobs[name]
	return ok
}

// Start starts the scheduler.
func (m *ScheduleManager) Start() {
	m.scheduler.Start()
}
