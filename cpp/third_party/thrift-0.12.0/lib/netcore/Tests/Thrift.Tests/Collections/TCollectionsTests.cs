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
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Collections;

namespace Thrift.Tests.Collections
{
    // ReSharper disable once InconsistentNaming
    [TestClass]
    public class TCollectionsTests
    {
        //TODO: Add tests for IEnumerable with objects and primitive values inside

        [TestMethod]
        public void TCollection_Equals_Primitive_Test()
        {
            var collection1 = new List<int> {1,2,3};
            var collection2 = new List<int> {1,2,3};

            var result = TCollections.Equals(collection1, collection2);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void TCollection_Equals_Primitive_Different_Test()
        {
            var collection1 = new List<int> { 1, 2, 3 };
            var collection2 = new List<int> { 1, 2 };

            var result = TCollections.Equals(collection1, collection2);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TCollection_Equals_Objects_Test()
        {
            var collection1 = new List<ExampleClass> { new ExampleClass { X = 1 }, new ExampleClass { X = 2 } };
            var collection2 = new List<ExampleClass> { new ExampleClass { X = 1 }, new ExampleClass { X = 2 } };

            var result = TCollections.Equals(collection1, collection2);

            // references to different collections
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TCollection_Equals_OneAndTheSameObject_Test()
        {
            var collection1 = new List<ExampleClass> { new ExampleClass { X = 1 }, new ExampleClass { X = 2 } };
            var collection2 = collection1;

            var result = TCollections.Equals(collection1, collection2);

            // references to one and the same collection
            Assert.IsTrue(result);
        }

        private class ExampleClass
        {
            public int X { get; set; }
        }
    }
}
