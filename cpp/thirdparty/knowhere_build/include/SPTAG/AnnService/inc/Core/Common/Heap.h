// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_HEAP_H_
#define _SPTAG_COMMON_HEAP_H_

namespace SPTAG
{
    namespace COMMON
    {

        // priority queue
        template <typename T>
        class Heap {
        public:
            Heap() : heap(nullptr), length(0), count(0) {}

            Heap(int size) { Resize(size); }

            void Resize(int size)
            {
                length = size;
                heap.reset(new T[length + 1]);  // heap uses 1-based indexing
                count = 0;
                lastlevel = int(pow(2.0, floor(log2(size))));
            }
            ~Heap() {}
            inline int size() { return count; }
            inline bool empty() { return count == 0; }
            inline void clear() { count = 0; }
            inline T& Top() { if (count == 0) return heap[0]; else return heap[1]; }

            // Insert a new element in the heap.
            void insert(T value)
            {
                /* If heap is full, then return without adding this element. */
                int loc;
                if (count == length) {
                    int maxi = lastlevel;
                    for (int i = lastlevel + 1; i <= length; i++)
                        if (heap[maxi] < heap[i]) maxi = i;
                    if (value > heap[maxi]) return;
                    loc = maxi;
                }
                else {
                    loc = ++(count);   /* Remember 1-based indexing. */
                }
                /* Keep moving parents down until a place is found for this node. */
                int par = (loc >> 1);                 /* Location of parent. */
                while (par > 0 && value < heap[par]) {
                    heap[loc] = heap[par];     /* Move parent down to loc. */
                    loc = par;
                    par >>= 1;
                }
                /* Insert the element at the determined location. */
                heap[loc] = value;
            }
            // Returns the node of minimum value from the heap (top of the heap).
            bool pop(T& value)
            {
                if (count == 0) return false;
                /* Switch first node with last. */
                value = heap[1];
                std::swap(heap[1], heap[count]);
                count--;
                heapify();      /* Move new node 1 to right position. */
                return true;  /* Return old last node. */
            }
            T& pop()
            {
                if (count == 0) return heap[0];
                /* Switch first node with last. */
                std::swap(heap[1], heap[count]);
                count--;
                heapify();      /* Move new node 1 to right position. */
                return heap[count + 1];  /* Return old last node. */
            }
        private:
            // Storage array for the heap.
            // Type T must be comparable.
            std::unique_ptr<T[]> heap;
            int length;
            int count; // Number of element in the heap
            int lastlevel;
            // Reorganizes the heap (a parent is smaller than its children) starting with a node.

            void heapify()
            {
                int parent = 1, next = 2;
                while (next < count) {
                    if (heap[next] > heap[next + 1]) next++;
                    if (heap[next] < heap[parent]) {
                        std::swap(heap[parent], heap[next]);
                        parent = next;
                        next <<= 1;
                    }
                    else break;
                }
                if (next == count && heap[next] < heap[parent]) std::swap(heap[parent], heap[next]);
            }
        };
    }
}

#endif // _SPTAG_COMMON_HEAP_H_
