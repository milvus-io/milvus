package configuration

type Manager interface {
	AddConfiguration(config ReplicateConfiguration) error
	GetConfiguration(replicateID string) (ReplicateConfiguration, error)
	RemoveConfiguration(replicateID string) error
	UpdateConfiguration(replicateID string, actions ...UpdateAction) error
	GetAllConfigurations() []ReplicateConfiguration
}

func NewManager() Manager {
	return nil
}

