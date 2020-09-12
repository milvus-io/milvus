from contextlib import contextmanager


def empty_server_interceptor_decorator(target_server, interceptor):
    return target_server


@contextmanager
def EmptySpan(*args, **kwargs):
    yield None
    return


class Tracer:
    def __init__(self,
                 tracer=None,
                 interceptor=None,
                 server_decorator=empty_server_interceptor_decorator):
        self.tracer = tracer
        self.interceptor = interceptor
        self.server_decorator = server_decorator

    def decorate(self, server):
        return self.server_decorator(server, self.interceptor)

    @property
    def empty(self):
        return self.tracer is None

    def close(self):
        self.tracer and self.tracer.close()

    def start_span(self,
                   operation_name=None,
                   child_of=None,
                   references=None,
                   tags=None,
                   start_time=None,
                   ignore_active_span=False):
        if self.empty:
            return EmptySpan()
        return self.tracer.start_span(operation_name, child_of, references,
                                      tags, start_time, ignore_active_span)
