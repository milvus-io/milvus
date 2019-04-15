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

namespace Thrift.Protocols.Utilities
{
    // ReSharper disable once InconsistentNaming
    internal static class TBase64Utils
    {
        //TODO: Constants
        //TODO: Check for args
        //TODO: Unitests

        internal const string EncodeTable = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        private static readonly int[] DecodeTable =
        {
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
            52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
            -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
            -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
        };

        internal static void Encode(byte[] src, int srcOff, int len, byte[] dst, int dstOff)
        {
            if (src == null)
            {
                throw new ArgumentNullException(nameof(src));
            }

            dst[dstOff] = (byte) EncodeTable[(src[srcOff] >> 2) & 0x3F];

            if (len == 3)
            {
                dst[dstOff + 1] = (byte) EncodeTable[((src[srcOff] << 4) & 0x30) | ((src[srcOff + 1] >> 4) & 0x0F)];
                dst[dstOff + 2] = (byte) EncodeTable[((src[srcOff + 1] << 2) & 0x3C) | ((src[srcOff + 2] >> 6) & 0x03)];
                dst[dstOff + 3] = (byte) EncodeTable[src[srcOff + 2] & 0x3F];
            }
            else if (len == 2)
            {
                dst[dstOff + 1] = (byte) EncodeTable[((src[srcOff] << 4) & 0x30) | ((src[srcOff + 1] >> 4) & 0x0F)];
                dst[dstOff + 2] = (byte) EncodeTable[(src[srcOff + 1] << 2) & 0x3C];
            }
            else
            {
                // len == 1
                dst[dstOff + 1] = (byte) EncodeTable[(src[srcOff] << 4) & 0x30];
            }
        }

        internal static void Decode(byte[] src, int srcOff, int len, byte[] dst, int dstOff)
        {
            if (src == null)
            {
                throw new ArgumentNullException(nameof(src));
            }

            dst[dstOff] = (byte) ((DecodeTable[src[srcOff] & 0x0FF] << 2) | (DecodeTable[src[srcOff + 1] & 0x0FF] >> 4));

            if (len > 2)
            {
                dst[dstOff + 1] =
                    (byte)
                    (((DecodeTable[src[srcOff + 1] & 0x0FF] << 4) & 0xF0) | (DecodeTable[src[srcOff + 2] & 0x0FF] >> 2));
                if (len > 3)
                {
                    dst[dstOff + 2] =
                        (byte)
                        (((DecodeTable[src[srcOff + 2] & 0x0FF] << 6) & 0xC0) | DecodeTable[src[srcOff + 3] & 0x0FF]);
                }
            }
        }
    }
}