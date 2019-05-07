# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################

lz4_root_dir = '../../..'

#examples_c_args = ['-Wextra', '-Wundef', '-Wshadow', '-Wcast-align', '-Wstrict-prototypes']

printVersion = executable('printVersion',
  join_paths(lz4_root_dir, 'examples/printVersion.c'),
  dependencies: liblz4_dep,
  install: false)
doubleBuffer = executable('doubleBuffer',
  join_paths(lz4_root_dir, 'examples/blockStreaming_doubleBuffer.c'),
  dependencies: liblz4_dep,
  install: false)
dictionaryRandomAccess = executable('dictionaryRandomAccess',
  join_paths(lz4_root_dir, 'examples/dictionaryRandomAccess.c'),
  dependencies: liblz4_dep,
  install: false)
ringBuffer = executable('ringBuffer',
  join_paths(lz4_root_dir, 'examples/blockStreaming_ringBuffer.c'),
  dependencies: liblz4_dep,
  install: false)
ringBufferHC = executable('ringBufferHC',
  join_paths(lz4_root_dir, 'examples/HCStreaming_ringBuffer.c'),
  dependencies: liblz4_dep,
  install: false)
lineCompress = executable('lineCompress',
  join_paths(lz4_root_dir, 'examples/blockStreaming_lineByLine.c'),
  dependencies: liblz4_dep,
  install: false)
frameCompress = executable('frameCompress',
  join_paths(lz4_root_dir, 'examples/frameCompress.c'),
  dependencies: liblz4_dep,
  install: false)
compressFunctions = executable('compressFunctions',
  join_paths(lz4_root_dir, 'examples/compress_functions.c'),
  dependencies: liblz4_dep,
  install: false)
simpleBuffer = executable('simpleBuffer',
  join_paths(lz4_root_dir, 'examples/simple_buffer.c'),
  dependencies: liblz4_dep,
  install: false)
