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

using System.Collections;
using System.Collections.Generic;

namespace Thrift.Collections
{
    // ReSharper disable once InconsistentNaming
    public class THashSet<T> : ICollection<T>
    {
        private readonly HashSet<T> _set = new HashSet<T>();

        public int Count => _set.Count;

        public bool IsReadOnly => false;

        public void Add(T item)
        {
            _set.Add(item);
        }

        public void Clear()
        {
            _set.Clear();
        }

        public bool Contains(T item)
        {
            return _set.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _set.CopyTo(array, arrayIndex);
        }

        public IEnumerator GetEnumerator()
        {
            return _set.GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return ((IEnumerable<T>) _set).GetEnumerator();
        }

        public bool Remove(T item)
        {
            return _set.Remove(item);
        }
    }
}