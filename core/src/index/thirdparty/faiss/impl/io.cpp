/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <cstring>
#include <cassert>

#include <faiss/impl/io.h>
#include <faiss/impl/FaissAssert.h>


namespace faiss {


/***********************************************************************
 * IO functions
 ***********************************************************************/


int IOReader::fileno ()
{
    FAISS_THROW_MSG ("IOReader does not support memory mapping");
}

int IOWriter::fileno ()
{
    FAISS_THROW_MSG ("IOWriter does not support memory mapping");
}

/***********************************************************************
 * IO Vector
 ***********************************************************************/



size_t VectorIOWriter::operator()(
                const void *ptr, size_t size, size_t nitems)
{
    size_t bytes = size * nitems;
    if (bytes > 0) {
        size_t o = data.size();
        data.resize(o + bytes);
        memcpy (&data[o], ptr, size * nitems);
    }
    return nitems;
}

size_t VectorIOReader::operator()(
                  void *ptr, size_t size, size_t nitems)
{
    if (rp >= data.size()) return 0;
    size_t nremain = (data.size() - rp) / size;
    if (nremain < nitems) nitems = nremain;
    if (size * nitems > 0) {
        memcpy (ptr, &data[rp], size * nitems);
        rp += size * nitems;
    }
    return nitems;
}




/***********************************************************************
 * IO File
 ***********************************************************************/



FileIOReader::FileIOReader(FILE *rf): f(rf) {}

FileIOReader::FileIOReader(const char * fname)
{
    name = fname;
    f = fopen(fname, "rb");
    FAISS_THROW_IF_NOT_FMT (f, "could not open %s for reading: %s",
                            fname, strerror(errno));
    need_close = true;
}

FileIOReader::~FileIOReader()  {
    if (need_close) {
        int ret = fclose(f);
        if (ret != 0) {// we cannot raise and exception in the destructor
            fprintf(stderr, "file %s close error: %s",
                    name.c_str(), strerror(errno));
        }
    }
}

size_t FileIOReader::operator()(void *ptr, size_t size, size_t nitems) {
    return fread(ptr, size, nitems, f);
}

int FileIOReader::fileno()  {
    return ::fileno (f);
}


FileIOWriter::FileIOWriter(FILE *wf): f(wf) {}

FileIOWriter::FileIOWriter(const char * fname)
{
    name = fname;
    f = fopen(fname, "wb");
    FAISS_THROW_IF_NOT_FMT (f, "could not open %s for writing: %s",
                            fname, strerror(errno));
    need_close = true;
}

FileIOWriter::~FileIOWriter()  {
    if (need_close) {
        int ret = fclose(f);
        if (ret != 0) {
            // we cannot raise and exception in the destructor
            fprintf(stderr, "file %s close error: %s",
                    name.c_str(), strerror(errno));
        }
    }
}

size_t FileIOWriter::operator()(const void *ptr, size_t size, size_t nitems) {
    return fwrite(ptr, size, nitems, f);
}

int FileIOWriter::fileno()  {
    return ::fileno (f);
}

uint32_t fourcc (const char sx[4]) {
    assert(4 == strlen(sx));
    const unsigned char *x = (unsigned char*)sx;
    return x[0] | x[1] << 8 | x[2] << 16 | x[3] << 24;
}


} // namespace faiss
