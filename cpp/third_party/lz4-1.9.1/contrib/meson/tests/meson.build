# #############################################################################
# Copyright (c) 2018-present    lzutao <taolzu(at)gmail.com>
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# #############################################################################

lz4_root_dir = '../../..'
programs_dir_inc = include_directories(join_paths(lz4_root_dir, 'programs'))
lib_dir_inc = include_directories(join_paths(lz4_root_dir, 'lib'))

# =============================================================================
# Test flags
# =============================================================================

TEST_FILES   = join_paths(meson.current_source_dir(), lz4_root_dir, 'tests/COPYING')
FUZZER_TIME  = '-T90s'
NB_LOOPS     = '-i1'

# =============================================================================
# Executables
# =============================================================================

fullbench_sources = [join_paths(lz4_root_dir, 'tests/fullbench.c')]
fullbench = executable('fullbench',
  fullbench_sources,
  include_directories: programs_dir_inc,
  dependencies: liblz4_dep,
  install: false)

fuzzer_sources = [join_paths(lz4_root_dir, 'tests/fuzzer.c')]
fuzzer = executable('fuzzer',
  fuzzer_sources,
  c_args: ['-D_DEFAULT_SOURCE', '-D_BSD_SOURCE'], # since glibc 2.19
  include_directories: programs_dir_inc,
  dependencies: liblz4_dep,
  install: false)

frametest_sources = [join_paths(lz4_root_dir, 'tests/frametest.c')]
frametest = executable('frametest',
  frametest_sources,
  include_directories: programs_dir_inc,
  dependencies: liblz4_dep,
  install: false)

roundTripTest_sources = [join_paths(lz4_root_dir, 'tests/roundTripTest.c')]
roundTripTest = executable('roundTripTest',
  roundTripTest_sources,
  dependencies: [ liblz4_dep ],
  install: false)

datagen_sources = [join_paths(lz4_root_dir, 'tests/datagencli.c')]
datagen = executable('datagen',
  datagen_sources,
  objects: lz4.extract_objects(join_paths(lz4_root_dir, 'programs/datagen.c')),
  include_directories: lz4_includes,
  dependencies: [ liblz4_dep ],
  install: false)

checkFrame_sources = [join_paths(lz4_root_dir, 'tests/checkFrame.c')]
checkFrame = executable('checkFrame',
  checkFrame_sources,
  include_directories: programs_dir_inc,
  dependencies: [ liblz4_dep ],
  install: false)

checkTag_sources = [join_paths(lz4_root_dir, 'tests/checkTag.c')]
checkTag = executable('checkTag',
  checkTag_sources,
  include_directories: lib_dir_inc,
  install: false)

# =============================================================================
# Tests (Use "meson test --list" to list all tests)
# =============================================================================

# XXX: (Need TEST) These timeouts (in seconds) when running on a HDD should be
# at least six times bigger than on a SSD

test('test-fullbench',
  fullbench,
  args: ['--no-prompt', NB_LOOPS, TEST_FILES],
  timeout: 420) # Should enough when running on HDD
test('test-fuzzer',
  fuzzer,
  args: [FUZZER_TIME],
  timeout: 100)
test('test-frametest',
  frametest,
  args: [FUZZER_TIME],
  timeout: 100)
