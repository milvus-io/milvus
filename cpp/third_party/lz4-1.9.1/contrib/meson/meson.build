# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################

project('lz4', ['c'],
  license: ['BSD', 'GPLv2'],
  default_options : ['c_std=c99',
    'buildtype=release'],
  version: '1.8.3',
  meson_version: '>=0.47.0')

cc = meson.get_compiler('c')
pkgconfig = import('pkgconfig')
python3 = import('python').find_installation()
c_std = get_option('c_std')
default_library = get_option('default_library')

host_machine_os = host_machine.system()
os_windows = 'windows'
os_linux = 'linux'
os_darwin = 'darwin'
os_freebsd = 'freebsd'
os_sun = 'sunos'

cc_id = cc.get_id()
compiler_gcc = 'gcc'
compiler_clang = 'clang'
compiler_msvc = 'msvc'

lz4_version = meson.project_version()

lz4_h_file = join_paths(meson.current_source_dir(), '../../lib/lz4.h')
GetLz4LibraryVersion_py = files('GetLz4LibraryVersion.py')
r = run_command(python3, GetLz4LibraryVersion_py, lz4_h_file)
if r.returncode() == 0
  output = r.stdout().strip()
  if output.version_compare('>@0@'.format(lz4_version))
    lz4_version = output
    message('Project version is now: @0@'.format(lz4_version))
  endif
else
  warning('Cannot find project version in @0@'.format(lz4_h_file))
endif

lz4_libversion = lz4_version

# =============================================================================
# Installation directories
# =============================================================================

lz4_prefix = get_option('prefix')
lz4_bindir = get_option('bindir')
lz4_datadir = get_option('datadir')
lz4_mandir = get_option('mandir')
lz4_docdir = join_paths(lz4_datadir, 'doc', meson.project_name())

# =============================================================================
# Project options
# =============================================================================

buildtype = get_option('buildtype')

# Built-in options
use_debug = get_option('debug')

# Custom options
debug_level = get_option('debug_level')
use_backtrace = get_option('backtrace')

build_programs = get_option('build_programs')
build_contrib = get_option('build_contrib')
build_tests = get_option('build_tests')
build_examples = get_option('build_examples')
#feature_multi_thread = get_option('multi_thread')

# =============================================================================
# Dependencies
# =============================================================================

#libm_dep = cc.find_library('m', required: build_tests)
#thread_dep = dependency('threads', required: feature_multi_thread)
#use_multi_thread = thread_dep.found()

# =============================================================================
# Compiler flags
# =============================================================================

add_project_arguments(['-DXXH_NAMESPACE=LZ4_'], language: 'c')

if [compiler_gcc, compiler_clang].contains(cc_id)
  common_warning_flags = []
  # Should use Meson's own --werror build option
  #common_warning_flags += ['-Werror']
  if c_std == 'c89' or c_std == 'gnu89'
    common_warning_flags += ['-pedantic', '-Wno-long-long', '-Wno-variadic-macros']
  elif c_std == 'c99' or c_std == 'gnu99'
    common_warning_flags += ['-pedantic']
  endif
  cc_compile_flags = cc.get_supported_arguments(common_warning_flags)
  add_project_arguments(cc_compile_flags, language: 'c')
endif

# =============================================================================
# Subdirs
# =============================================================================

subdir('lib')

if build_programs
  subdir('programs')
endif

if build_tests
  subdir('tests')
endif

if build_contrib
  subdir('contrib')
endif

if build_examples
  subdir('examples')
endif
