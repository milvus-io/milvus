class ParamError(ValueError):
    pass


class ConnectError(ValueError):
    pass


class NotConnectError(ConnectError):
    pass


class RepeatingConnectError(ConnectError):
    pass


class DisconnectNotConnectedClientError(ValueError):
    pass
