# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from thrift.Thrift import TType

TYPE_IDX = 1
SPEC_ARGS_IDX = 3
SPEC_ARGS_CLASS_REF_IDX = 0
SPEC_ARGS_THRIFT_SPEC_IDX = 1


def fix_spec(all_structs):
    """Wire up recursive references for all TStruct definitions inside of each thrift_spec."""
    for struc in all_structs:
        spec = struc.thrift_spec
        for thrift_spec in spec:
            if thrift_spec is None:
                continue
            elif thrift_spec[TYPE_IDX] == TType.STRUCT:
                other = thrift_spec[SPEC_ARGS_IDX][SPEC_ARGS_CLASS_REF_IDX].thrift_spec
                thrift_spec[SPEC_ARGS_IDX][SPEC_ARGS_THRIFT_SPEC_IDX] = other
            elif thrift_spec[TYPE_IDX] in (TType.LIST, TType.SET):
                _fix_list_or_set(thrift_spec[SPEC_ARGS_IDX])
            elif thrift_spec[TYPE_IDX] == TType.MAP:
                _fix_map(thrift_spec[SPEC_ARGS_IDX])


def _fix_list_or_set(element_type):
    # For a list or set, the thrift_spec entry looks like,
    # (1, TType.LIST, 'lister', (TType.STRUCT, [RecList, None], False), None, ),  # 1
    # so ``element_type`` will be,
    # (TType.STRUCT, [RecList, None], False)
    if element_type[0] == TType.STRUCT:
        element_type[1][1] = element_type[1][0].thrift_spec
    elif element_type[0] in (TType.LIST, TType.SET):
        _fix_list_or_set(element_type[1])
    elif element_type[0] == TType.MAP:
        _fix_map(element_type[1])


def _fix_map(element_type):
    # For a map of key -> value type, ``element_type`` will be,
    # (TType.I16, None, TType.STRUCT, [RecMapBasic, None], False), None, )
    # which is just a normal struct definition.
    #
    # For a map of key -> list / set, ``element_type`` will be,
    # (TType.I16, None, TType.LIST, (TType.STRUCT, [RecMapList, None], False), False)
    # and we need to process the 3rd element as a list.
    #
    # For a map of key -> map, ``element_type`` will be,
    # (TType.I16, None, TType.MAP, (TType.I16, None, TType.STRUCT,
    #  [RecMapMap, None], False), False)
    # and need to process 3rd element as a map.

    # Is the map key a struct?
    if element_type[0] == TType.STRUCT:
        element_type[1][1] = element_type[1][0].thrift_spec
    elif element_type[0] in (TType.LIST, TType.SET):
        _fix_list_or_set(element_type[1])
    elif element_type[0] == TType.MAP:
        _fix_map(element_type[1])

    # Is the map value a struct?
    if element_type[2] == TType.STRUCT:
        element_type[3][1] = element_type[3][0].thrift_spec
    elif element_type[2] in (TType.LIST, TType.SET):
        _fix_list_or_set(element_type[3])
    elif element_type[2] == TType.MAP:
        _fix_map(element_type[3])
