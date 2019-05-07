# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################

lz4_root_dir = '../../../..'

add_languages('cpp')
cxx = meson.get_compiler('cpp')

gen_manual_includes = include_directories(join_paths(lz4_root_dir, 'contrib/gen_manual'))

gen_manual_cppflags = cxx.get_supported_arguments(['-Wextra', '-Wcast-qual',
  '-Wcast-align', '-Wshadow', '-Wstrict-aliasing=1', '-Wswitch-enum',
  '-Wno-comment'])

gen_manual = executable('gen_manual',
  join_paths(lz4_root_dir, 'contrib/gen_manual/gen_manual.cpp'),
  cpp_args: gen_manual_cppflags,
  include_directories: gen_manual_includes,
  install: false)

# Update lz4 manual
lz4_manual_html = custom_target('lz4_manual.html',
  output : 'lz4_manual.html',
  command : [gen_manual,
    lz4_version,
    join_paths(meson.current_source_dir(), lz4_root_dir, 'lib/lz4.h'),
    '@OUTPUT@'],
  install : false)
# Update lz4frame manual
lz4_manual_html = custom_target('lz4frame_manual.html',
  output : 'lz4frame_manual.html',
  command : [gen_manual,
    lz4_version,
    join_paths(meson.current_source_dir(), lz4_root_dir, 'lib/lz4frame.h'),
    '@OUTPUT@'],
  install : false)
