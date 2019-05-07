# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################

lz4_root_dir = '../../..'

lz4_includes = include_directories(join_paths(lz4_root_dir, 'programs'))
lz4_sources = [join_paths(lz4_root_dir, 'programs/bench.c'),
  join_paths(lz4_root_dir, 'programs/datagen.c'),
  join_paths(lz4_root_dir, 'programs/lz4cli.c'),
  join_paths(lz4_root_dir, 'programs/lz4io.c')]
lz4_c_args = []

export_dynamic_on_windows = false
# explicit backtrace enable/disable for Linux & Darwin
if not use_backtrace
  lz4_c_args += '-DBACKTRACE_ENABLE=0'
elif use_debug and host_machine_os == os_windows  # MinGW target
  lz4_c_args += '-DBACKTRACE_ENABLE=1'
  export_dynamic_on_windows = true
endif

lz4_deps = [ liblz4_dep ]

lz4 = executable('lz4',
  lz4_sources,
  include_directories: lz4_includes,
  c_args: lz4_c_args,
  dependencies: lz4_deps,
  export_dynamic: export_dynamic_on_windows, # Since Meson 0.45.0
  install: true)

# =============================================================================
# Programs and manpages installing
# =============================================================================

install_man(join_paths(lz4_root_dir, 'programs/lz4.1'))

InstallSymlink_py = '../InstallSymlink.py'
lz4_man1_dir = join_paths(lz4_mandir, 'man1')
bin_EXT = host_machine_os == os_windows ? '.exe' : ''
man1_EXT = meson.version().version_compare('>=0.49.0') ? '.1' : '.1.gz'

foreach f : ['lz4c', 'lz4cat', 'unlz4']
  meson.add_install_script(InstallSymlink_py, 'lz4' + bin_EXT, f + bin_EXT, lz4_bindir)
  meson.add_install_script(InstallSymlink_py, 'lz4' + man1_EXT, f + man1_EXT, lz4_man1_dir)
endforeach
