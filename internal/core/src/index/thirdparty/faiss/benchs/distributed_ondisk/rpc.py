# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python3

"""
Simplistic RPC implementation.
Exposes all functions of a Server object.

Uses pickle for serialization and the socket interface.
"""

import os,pdb,pickle,time,errno,sys,_thread,traceback,socket,threading,gc


# default
PORT=12032


#########################################################################
# simple I/O functions



def inline_send_handle(f, conn):
    st = os.fstat(f.fileno())
    size = st.st_size
    pickle.dump(size, conn)
    conn.write(f.read(size))

def inline_send_string(s, conn):
    size = len(s)
    pickle.dump(size, conn)
    conn.write(s)


class FileSock:
    " wraps a socket so that it is usable by pickle/cPickle "

    def __init__(self,sock):
        self.sock = sock
        self.nr=0

    def write(self, buf):
        # print("sending %d bytes"%len(buf))
        #self.sock.sendall(buf)
        # print("...done")
        bs = 512 * 1024
        ns = 0
        while ns < len(buf):
            sent = self.sock.send(buf[ns:ns + bs])
            ns += sent


    def read(self,bs=512*1024):
        #if self.nr==10000: pdb.set_trace()
        self.nr+=1
        # print("read bs=%d"%bs)
        b = []
        nb = 0
        while len(b)<bs:
            # print('   loop')
            rb = self.sock.recv(bs - nb)
            if not rb: break
            b.append(rb)
            nb += len(rb)
        return b''.join(b)

    def readline(self):
        # print("readline!")
        """may be optimized..."""
        s=bytes()
        while True:
            c=self.read(1)
            s+=c
        if len(c)==0 or chr(c[0])=='\n':
            return s

class ClientExit(Exception):
    pass

class ServerException(Exception):
    pass


class Server:
    """
    server protocol. Methods from classes that subclass Server can be called
    transparently from a client
    """

    def __init__(self, s, logf=sys.stderr, log_prefix=''):
        self.logf = logf
        self.log_prefix = log_prefix

        # connection

        self.conn = s
        self.fs = FileSock(s)


    def log(self, s):
        self.logf.write("Sever log %s: %s\n" % (self.log_prefix, s))

    def one_function(self):
        """
        Executes a single function with associated I/O.
        Protocol:
        - the arguments and results are serialized with the pickle protocol
        - client sends : (fname,args)
            fname = method name to call
            args = tuple of arguments
        - server sends result: (rid,st,ret)
            rid = request id
            st = None, or exception if there was during execution
            ret = return value or None if st!=None
        """

        try:
            (fname,args)=pickle.load(self.fs)
        except EOFError:
            raise ClientExit("read args")
        self.log("executing method %s"%(fname))
        st = None
        ret = None
        try:
            f=getattr(self,fname)
        except AttributeError:
            st = AttributeError("unknown method "+fname)
            self.log("unknown method ")

        try:
            ret = f(*args)
        except Exception as e:
            # due to a bug (in mod_python?), ServerException cannot be
            # unpickled, so send the string and make the exception on the client side

            #st=ServerException(
            #  "".join(traceback.format_tb(sys.exc_info()[2]))+
            #  str(e))
            st="".join(traceback.format_tb(sys.exc_info()[2]))+str(e)
            self.log("exception in method")
            traceback.print_exc(50,self.logf)
            self.logf.flush()

        print("return")
        try:
            pickle.dump((st ,ret), self.fs, protocol=4)
        except EOFError:
            raise ClientExit("function return")

    def exec_loop(self):
        """ main execution loop. Loops and handles exit states"""

        self.log("in exec_loop")
        try:
            while True:
                self.one_function()
        except ClientExit as e:
            self.log("ClientExit %s"%e)
        except socket.error as e:
            self.log("socket error %s"%e)
            traceback.print_exc(50,self.logf)
        except EOFError:
            self.log("EOF during communication")
            traceback.print_exc(50,self.logf)
        except BaseException:
            # unexpected
            traceback.print_exc(50,sys.stderr)
            sys.exit(1)

        print("exit sever")

    def exec_loop_cleanup(self):
        pass

    ###################################################################
    # spying stuff

    def get_ps_stats(self):
        ret=''
        f=os.popen("echo ============ `hostname` uptime:; uptime;"+
                   "echo ============ self:; "+
                   "ps -p %d -o pid,vsize,rss,%%cpu,nlwp,psr; "%os.getpid()+
                   "echo ============ run queue:;"+
                   "ps ar -o user,pid,%cpu,%mem,ni,nlwp,psr,vsz,rss,cputime,command")
        for l in f:
            ret+=l
        return ret

class Client:
    """
    Methods of the server object can be called transparently. Exceptions are
    re-raised.
    """
    def __init__(self, HOST, port=PORT, v6=False):
        socktype = socket.AF_INET6 if v6 else socket.AF_INET

        sock = socket.socket(socktype, socket.SOCK_STREAM)
        print("connecting",HOST, port, socktype)
        sock.connect((HOST, port))
        self.sock = sock
        self.fs = FileSock(sock)

    def generic_fun(self, fname, args):
        # int "gen fun",fname
        pickle.dump((fname, args), self.fs, protocol=4)
        return self.get_result()

    def get_result(self):
        (st, ret) = pickle.load(self.fs)
        if st!=None:
            raise ServerException(st)
        else:
            return ret

    def __getattr__(self,name):
        return lambda *x: self.generic_fun(name,x)


def run_server(new_handler, port=PORT, report_to_file=None, v6=False):

    HOST = ''                 # Symbolic name meaning the local host
    socktype = socket.AF_INET6 if v6 else socket.AF_INET
    s = socket.socket(socktype, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    print("bind %s:%d" % (HOST, port))
    s.bind((HOST, port))
    s.listen(5)

    print("accepting connections")
    if report_to_file is not None:
        print('storing host+port in', report_to_file)
        open(report_to_file, 'w').write('%s:%d ' % (socket.gethostname(), port))

    while True:
        try:
            conn, addr = s.accept()
        except socket.error as e:
            if e[1]=='Interrupted system call': continue
            raise

        print('Connected by', addr, end=' ')

        ibs = new_handler(conn)

        tid = _thread.start_new_thread(ibs.exec_loop,())

        print("tid",tid)
