# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python3

import numpy as np
import unittest
import faiss
import tempfile
import os
import io
import sys
import warnings
from multiprocessing.dummy import Pool as ThreadPool

from common import get_dataset, get_dataset_2


class TestIOVariants(unittest.TestCase):

    def test_io_error(self):
        d, n = 32, 1000
        x = np.random.uniform(size=(n, d)).astype('float32')
        index = faiss.IndexFlatL2(d)
        index.add(x)
        _, fname = tempfile.mkstemp()
        try:
            faiss.write_index(index, fname)

            # should be fine
            faiss.read_index(fname)

            # now damage file
            data = open(fname, 'rb').read()
            data = data[:int(len(data) / 2)]
            open(fname, 'wb').write(data)

            # should make a nice readable exception that mentions the filename
            try:
                faiss.read_index(fname)
            except RuntimeError as e:
                if fname not in str(e):
                    raise
            else:
                raise

        finally:
            if os.path.exists(fname):
                os.unlink(fname)


class TestCallbacks(unittest.TestCase):

    def do_write_callback(self, bsz):
        d, n = 32, 1000
        x = np.random.uniform(size=(n, d)).astype('float32')
        index = faiss.IndexFlatL2(d)
        index.add(x)

        f = io.BytesIO()
        # test with small block size
        writer = faiss.PyCallbackIOWriter(f.write, 1234)

        if bsz > 0:
            writer = faiss.BufferedIOWriter(writer, bsz)

        faiss.write_index(index, writer)
        del writer   # make sure all writes committed

        if sys.version_info[0] < 3:
            buf = f.getvalue()
        else:
            buf = f.getbuffer()

        index2 = faiss.deserialize_index(np.frombuffer(buf, dtype='uint8'))

        self.assertEqual(index.d, index2.d)
        self.assertTrue(np.all(
            faiss.vector_to_array(index.xb) == faiss.vector_to_array(index2.xb)
        ))

        # This is not a callable function: shoudl raise an exception
        writer = faiss.PyCallbackIOWriter("blabla")
        self.assertRaises(
            Exception,
            faiss.write_index, index, writer
        )

    def test_buf_read(self):
        x = np.random.uniform(size=20)

        _, fname = tempfile.mkstemp()
        try:
            x.tofile(fname)

            f = open(fname, 'rb')
            reader = faiss.PyCallbackIOReader(f.read, 1234)

            bsz = 123
            reader = faiss.BufferedIOReader(reader, bsz)

            y = np.zeros_like(x)
            print('nbytes=', y.nbytes)
            reader(faiss.swig_ptr(y), y.nbytes, 1)

            np.testing.assert_array_equal(x, y)
        finally:
            if os.path.exists(fname):
                os.unlink(fname)

    def do_read_callback(self, bsz):
        d, n = 32, 1000
        x = np.random.uniform(size=(n, d)).astype('float32')
        index = faiss.IndexFlatL2(d)
        index.add(x)

        _, fname = tempfile.mkstemp()
        try:
            faiss.write_index(index, fname)

            f = open(fname, 'rb')

            reader = faiss.PyCallbackIOReader(f.read, 1234)

            if bsz > 0:
                reader = faiss.BufferedIOReader(reader, bsz)

            index2 = faiss.read_index(reader)

            self.assertEqual(index.d, index2.d)
            np.testing.assert_array_equal(
                faiss.vector_to_array(index.xb),
                faiss.vector_to_array(index2.xb)
            )

            # This is not a callable function: should raise an exception
            reader = faiss.PyCallbackIOReader("blabla")
            self.assertRaises(
                Exception,
                faiss.read_index, reader
            )
        finally:
            if os.path.exists(fname):
                os.unlink(fname)

    def test_write_callback(self):
        self.do_write_callback(0)

    def test_write_buffer(self):
        self.do_write_callback(123)
        self.do_write_callback(2345)

    def test_read_callback(self):
        self.do_read_callback(0)

    def test_read_callback_buffered(self):
        self.do_read_callback(123)
        self.do_read_callback(12345)

    def test_read_buffer(self):
        d, n = 32, 1000
        x = np.random.uniform(size=(n, d)).astype('float32')
        index = faiss.IndexFlatL2(d)
        index.add(x)

        _, fname = tempfile.mkstemp()
        try:
            faiss.write_index(index, fname)

            reader = faiss.BufferedIOReader(
                faiss.FileIOReader(fname), 1234)

            index2 = faiss.read_index(reader)

            self.assertEqual(index.d, index2.d)
            np.testing.assert_array_equal(
                faiss.vector_to_array(index.xb),
                faiss.vector_to_array(index2.xb)
            )

        finally:
            if os.path.exists(fname):
                os.unlink(fname)


    def test_transfer_pipe(self):
        """ transfer an index through a Unix pipe """

        d, n = 32, 1000
        x = np.random.uniform(size=(n, d)).astype('float32')
        index = faiss.IndexFlatL2(d)
        index.add(x)
        Dref, Iref = index.search(x, 10)

        rf, wf = os.pipe()

        # start thread that will decompress the index

        def index_from_pipe():
            reader = faiss.PyCallbackIOReader(lambda size: os.read(rf, size))
            return faiss.read_index(reader)

        fut = ThreadPool(1).apply_async(index_from_pipe, ())

        # write to pipe
        writer = faiss.PyCallbackIOWriter(lambda b: os.write(wf, b))
        faiss.write_index(index, writer)

        index2 = fut.get()

        # closing is not really useful but it does not hurt
        os.close(wf)
        os.close(rf)

        Dnew, Inew = index2.search(x, 10)

        np.testing.assert_array_equal(Iref, Inew)
        np.testing.assert_array_equal(Dref, Dnew)
