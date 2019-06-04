class ParamError(ValueError):
    pass


class ConnectParamMissingError(ParamError):
    pass


class RepeatingConnectError(ValueError):
    pass


class DisconnectNotConnectedClientError(ValueError):
    pass
