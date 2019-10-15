// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include "inc/Helper/StringConvert.h"

using namespace System;
using namespace System::Runtime::InteropServices;

namespace Microsoft
{
    namespace ANN
    {
        namespace SPTAGManaged
        {
            ///<summary>
            /// hold a pointer to an umnanaged object from the core project
            ///</summary>
            template<class T>
            public ref class ManagedObject
            {
            protected:
                T* m_Instance;

            public:
                ManagedObject(T* instance)
                    :m_Instance(instance)
                {
                }

                ManagedObject(T& instance)
                {
                    m_Instance = new T(instance);
                }

                /// <summary>
                /// destructor, which is called whenever delete an object with delete keyword
                /// </summary>
                virtual ~ManagedObject()
                {
                    if (m_Instance != nullptr)
                    {
                        delete m_Instance;
                    }
                }

                /// <summary>
                /// finalizer which is called by Garbage Collector whenever it destroys the wrapper object.
                /// </summary>
                !ManagedObject()
                {
                    if (m_Instance != nullptr)
                    {
                        delete m_Instance;
                    }
                }

                T* GetInstance()
                {
                    return m_Instance;
                }

                static const char* string_to_char_array(String^ string)
                {
                    const char* str = (const char*)(Marshal::StringToHGlobalAnsi(string)).ToPointer();
                    return str;
                }

                template<typename T>
                static T string_to(String^ string)
                {
                    T data;
                    SPTAG::Helper::Convert::ConvertStringTo<T>(string_to_char_array(string), data);
                    return data;
                }
            };
        }
    }
}

