package framework

// Connector defines the interface for a Gofka Connect connector.
type Connector interface {
	// Start initializes the connector.
	Start(config map[string]string)
	// Stop stops the connector.
	Stop()
	// TaskConfigs returns a list of task configurations that the connector will run.
	TaskConfigs(maxTasks int) []map[string]string
	// TaskClass returns the class of the Task that should be used with this connector.
	TaskClass() Task
}
