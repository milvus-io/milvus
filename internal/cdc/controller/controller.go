package controller

// Controller controls and schedules the CDC process.
// It will periodically check and schedule the replicate configurations.
type Controller interface {
	Start()
	Stop()
}
