package framework

// Task defines the interface for a Gofka Connect task.
type Task interface {
	// Start initializes the task.
	Start(config map[string]string)
	// Stop stops the task.
	Stop()
}
