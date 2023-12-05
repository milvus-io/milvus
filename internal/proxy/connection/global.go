package connection

import "sync"

var connectionManagerInstance *connectionManager

var getConnectionManagerInstanceOnce sync.Once

func GetManager() *connectionManager {
	getConnectionManagerInstanceOnce.Do(func() {
		connectionManagerInstance = newConnectionManager(
			withDuration(defaultConnCheckDuration),
			withTTL(defaultTTLForInactiveConn))
	})
	return connectionManagerInstance
}
