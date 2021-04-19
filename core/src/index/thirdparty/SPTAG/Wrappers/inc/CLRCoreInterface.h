// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include "ManagedObject.h"
#include "inc/Core/VectorIndex.h"

using namespace System;

namespace Microsoft
{
    namespace ANN
    {
        namespace SPTAGManaged
        {

            public ref class BasicResult :
                public ManagedObject<SPTAG::BasicResult>
            {
            public:
                BasicResult(SPTAG::BasicResult* p_instance) : ManagedObject(p_instance)
                {
                }

                property int VID
                {
                public:
                    int get()
                    {
                        return m_Instance->VID;
                    }
                private:
                    void set(int p_vid)
                    {
                    }
                }

                property float Dist
                {
                public:
                    float get()
                    {
                        return m_Instance->Dist;
                    }
                private:
                    void set(float p_dist)
                    {
                    }
                }

                property array<Byte>^ Meta
                {
                public:
                    array<Byte>^ get()
                    {
                        array<Byte>^ buf = gcnew array<Byte>(m_Instance->Meta.Length());
                        Marshal::Copy((IntPtr)m_Instance->Meta.Data(), buf, 0, (int)m_Instance->Meta.Length());
                        return buf;
                    }
                private:
                    void set(array<Byte>^ p_meta)
                    {
                    }
                }
            };

            public ref class AnnIndex :
                public ManagedObject<std::shared_ptr<SPTAG::VectorIndex>>
            {
            public:
                AnnIndex(std::shared_ptr<SPTAG::VectorIndex> p_index);

                AnnIndex(String^ p_algoType, String^ p_valueType, int p_dimension);

                void SetBuildParam(String^ p_name, String^ p_value);

                void SetSearchParam(String^ p_name, String^ p_value);

                bool Build(array<Byte>^ p_data, int p_num);

                bool BuildWithMetaData(array<Byte>^ p_data, array<Byte>^ p_meta, int p_num, bool p_withMetaIndex);

                array<BasicResult^>^ Search(array<Byte>^ p_data, int p_resultNum);

                array<BasicResult^>^ SearchWithMetaData(array<Byte>^ p_data, int p_resultNum);

                bool Save(String^ p_saveFile);

                array<array<Byte>^>^ Dump();

                bool Add(array<Byte>^ p_data, int p_num);

                bool AddWithMetaData(array<Byte>^ p_data, array<Byte>^ p_meta, int p_num);

                bool Delete(array<Byte>^ p_data, int p_num);

                bool DeleteByMetaData(array<Byte>^ p_meta);

                static AnnIndex^ Load(String^ p_loaderFile);

                static AnnIndex^ Load(array<array<Byte>^>^ p_index);

                static bool Merge(String^ p_indexFilePath1, String^ p_indexFilePath2);

            private:

                int m_dimension;

                size_t m_inputVectorSize;
            };
        }
    }
}
