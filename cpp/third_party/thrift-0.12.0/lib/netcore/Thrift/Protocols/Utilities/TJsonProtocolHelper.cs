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

using Thrift.Protocols.Entities;

namespace Thrift.Protocols.Utilities
{
    // ReSharper disable once InconsistentNaming
    public static class TJSONProtocolHelper
    {
        public static byte[] GetTypeNameForTypeId(TType typeId)
        {
            switch (typeId)
            {
                case TType.Bool:
                    return TJSONProtocolConstants.TypeNames.NameBool;
                case TType.Byte:
                    return TJSONProtocolConstants.TypeNames.NameByte;
                case TType.I16:
                    return TJSONProtocolConstants.TypeNames.NameI16;
                case TType.I32:
                    return TJSONProtocolConstants.TypeNames.NameI32;
                case TType.I64:
                    return TJSONProtocolConstants.TypeNames.NameI64;
                case TType.Double:
                    return TJSONProtocolConstants.TypeNames.NameDouble;
                case TType.String:
                    return TJSONProtocolConstants.TypeNames.NameString;
                case TType.Struct:
                    return TJSONProtocolConstants.TypeNames.NameStruct;
                case TType.Map:
                    return TJSONProtocolConstants.TypeNames.NameMap;
                case TType.Set:
                    return TJSONProtocolConstants.TypeNames.NameSet;
                case TType.List:
                    return TJSONProtocolConstants.TypeNames.NameList;
                default:
                    throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED, "Unrecognized exType");
            }
        }

        public static TType GetTypeIdForTypeName(byte[] name)
        {
            var result = TType.Stop;
            if (name.Length > 1)
            {
                switch (name[0])
                {
                    case (byte) 'd':
                        result = TType.Double;
                        break;
                    case (byte) 'i':
                        switch (name[1])
                        {
                            case (byte) '8':
                                result = TType.Byte;
                                break;
                            case (byte) '1':
                                result = TType.I16;
                                break;
                            case (byte) '3':
                                result = TType.I32;
                                break;
                            case (byte) '6':
                                result = TType.I64;
                                break;
                        }
                        break;
                    case (byte) 'l':
                        result = TType.List;
                        break;
                    case (byte) 'm':
                        result = TType.Map;
                        break;
                    case (byte) 'r':
                        result = TType.Struct;
                        break;
                    case (byte) 's':
                        if (name[1] == (byte) 't')
                        {
                            result = TType.String;
                        }
                        else if (name[1] == (byte) 'e')
                        {
                            result = TType.Set;
                        }
                        break;
                    case (byte) 't':
                        result = TType.Bool;
                        break;
                }
            }
            if (result == TType.Stop)
            {
                throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED, "Unrecognized exType");
            }
            return result;
        }

        /// <summary>
        ///     Return true if the given byte could be a valid part of a JSON number.
        /// </summary>
        public static bool IsJsonNumeric(byte b)
        {
            switch (b)
            {
                case (byte)'+':
                case (byte)'-':
                case (byte)'.':
                case (byte)'0':
                case (byte)'1':
                case (byte)'2':
                case (byte)'3':
                case (byte)'4':
                case (byte)'5':
                case (byte)'6':
                case (byte)'7':
                case (byte)'8':
                case (byte)'9':
                case (byte)'E':
                case (byte)'e':
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        ///     Convert a byte containing a hex char ('0'-'9' or 'a'-'f') into its
        ///     corresponding hex value
        /// </summary>
        public static byte ToHexVal(byte ch)
        {
            if (ch >= '0' && ch <= '9')
            {
                return (byte)((char)ch - '0');
            }

            if (ch >= 'a' && ch <= 'f')
            {
                ch += 10;
                return (byte)((char)ch - 'a');
            }

            throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected hex character");
        }

        /// <summary>
        ///     Convert a byte containing a hex value to its corresponding hex character
        /// </summary>
        public static byte ToHexChar(byte val)
        {
            val &= 0x0F;
            if (val < 10)
            {
                return (byte)((char)val + '0');
            }
            val -= 10;
            return (byte)((char)val + 'a');
        }
    }
}