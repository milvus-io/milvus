// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_WORKSPACEPOOL_H_
#define _SPTAG_COMMON_WORKSPACEPOOL_H_

#include "WorkSpace.h"

#include <list>
#include <mutex>

namespace SPTAG
{
namespace COMMON
{

class WorkSpacePool
{
public:
    WorkSpacePool(int p_maxCheck, SizeType p_vectorCount);

    virtual ~WorkSpacePool();

    std::shared_ptr<WorkSpace> Rent();

    void Return(const std::shared_ptr<WorkSpace>& p_workSpace);

    void Init(int size);

private:
    std::list<std::shared_ptr<WorkSpace>> m_workSpacePool;

    std::mutex m_workSpacePoolMutex;

    int m_maxCheck;

    SizeType m_vectorCount;
};

}
}

#endif // _SPTAG_COMMON_WORKSPACEPOOL_H_
