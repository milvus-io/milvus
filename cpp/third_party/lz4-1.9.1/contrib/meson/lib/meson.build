# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################

lz4_root_dir = '../../..'

liblz4_includes = [include_directories(join_paths(lz4_root_dir, 'lib'))]
liblz4_sources = [join_paths(lz4_root_dir, 'lib/lz4.c'),
  join_paths(lz4_root_dir, 'lib/lz4frame.c'),
  join_paths(lz4_root_dir, 'lib/lz4hc.c'),
  join_paths(lz4_root_dir, 'lib/xxhash.c')]
liblz4_c_args = []

liblz4_debug_cflags = []
if use_debug
  liblz4_c_args += '-DLZ4_DEBUG=@0@'.format(debug_level)
  if [compiler_gcc, compiler_clang].contains(cc_id)
    liblz4_debug_cflags = ['-Wextra', '-Wcast-qual', '-Wcast-align', '-Wshadow',
      '-Wswitch-enum', '-Wdeclaration-after-statement', '-Wstrict-prototypes',
      '-Wundef', '-Wpointer-arith', '-Wstrict-aliasing=1']
  endif
endif
liblz4_c_args += cc.get_supported_arguments(liblz4_debug_cflags)

if host_machine_os == os_windows and default_library != 'static'
  liblz4_c_args += '-DLZ4_DLL_EXPORT=1'
endif

liblz4 = library('lz4',
  liblz4_sources,
  include_directories: liblz4_includes,
  c_args: liblz4_c_args,
  install: true,
  version: lz4_libversion)

liblz4_dep = declare_dependency(link_with: liblz4,
  include_directories: liblz4_includes)

pkgconfig.generate(liblz4,
  name: 'lz4',
  filebase: 'liblz4',
  description: 'extremely fast lossless compression algorithm library',
  version: lz4_libversion,
  url: 'http://www.lz4.org/')

install_headers(join_paths(lz4_root_dir, 'lib/lz4.h'),
  join_paths(lz4_root_dir, 'lib/lz4hc.h'),
  join_paths(lz4_root_dir, 'lib/lz4frame.h'))

if default_library != 'shared'
  install_headers(join_paths(lz4_root_dir, 'lib/lz4frame_static.h'))
endif
