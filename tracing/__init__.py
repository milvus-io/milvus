def empty_server_interceptor_decorator(target_server, interceptor):
    return target_server


class Tracer:
    def __init__(self, tracer=None,
                 interceptor=None,
                 server_decorator=empty_server_interceptor_decorator):
        self.tracer = tracer
        self.interceptor = interceptor
        self.server_decorator = server_decorator

    def decorate(self, server):
        return self.server_decorator(server, self.interceptor)

    def close(self):
        self.tracer and self.tracer.close()

    def start_span(self, operation_name=None,
                   child_of=None, references=None, tags=None,
                   start_time=None, ignore_active_span=False):
        return self.tracer.start_span(operation_name, child_of,
                                      references, tags, start_time, ignore_active_span)
