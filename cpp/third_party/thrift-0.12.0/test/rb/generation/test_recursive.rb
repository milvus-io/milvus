#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

require File.join(File.dirname(__FILE__), '../test_helper')
require 'recursive_types'

class TestRecursiveGeneration < Test::Unit::TestCase
  CHILD_ITEM = "child item"
  PARENT_ITEM = "parent item"

  def test_can_create_recursive_tree

    child_tree = RecTree.new
    child_tree.item = CHILD_ITEM

    parent_tree = RecTree.new
    parent_tree.item = PARENT_ITEM
    parent_tree.children = [child_tree]

    assert_equal(PARENT_ITEM, parent_tree.item)
    assert_equal(1, parent_tree.children.length)
    assert_equal(CHILD_ITEM, parent_tree.children.first.item)
    assert_nil(parent_tree.children.first.children)
  end
end
