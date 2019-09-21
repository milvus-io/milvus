def mark_grpc_method(func):
    setattr(func, 'grpc_method', True)
    return func
