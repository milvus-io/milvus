// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Collections;

namespace Thrift.Tests.Collections
{
    // ReSharper disable once InconsistentNaming
    [TestClass]
    public class THashSetTests
    {
        [TestMethod]
        public void THashSet_Equals_Primitive_Test()
        {
            const int value = 1;

            var hashSet = new THashSet<int> {value};
            
            Assert.IsTrue(hashSet.Contains(value));

            hashSet.Remove(value);

            Assert.IsTrue(hashSet.Count == 0);

            hashSet.Add(value);

            Assert.IsTrue(hashSet.Contains(value));

            hashSet.Clear();

            Assert.IsTrue(hashSet.Count == 0);

            var newArr = new int[1];
            hashSet.Add(value);
            hashSet.CopyTo(newArr, 0);

            Assert.IsTrue(newArr.Contains(value));

            var en = hashSet.GetEnumerator();
            en.MoveNext();

            Assert.IsTrue((int)en.Current == value);
            
            using (var ien = ((IEnumerable<int>)hashSet).GetEnumerator())
            {
                ien.MoveNext();

                Assert.IsTrue(ien.Current == value);
            }
        }
    }
}
