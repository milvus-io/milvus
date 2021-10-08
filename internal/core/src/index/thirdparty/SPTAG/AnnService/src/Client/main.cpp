// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Client/Options.h"
#include "inc/Client/ClientWrapper.h"

#include <cstdio>
#include <atomic>
#include <iostream>

std::unique_ptr<SPTAG::Client::ClientWrapper> g_client;

int main(int argc, char** argv)
{
    SPTAG::Client::ClientOptions options;
    if (!options.Parse(argc - 1, argv + 1))
    {
        return 1;
    }

    g_client.reset(new SPTAG::Client::ClientWrapper(options));
    if (!g_client->IsAvailable())
    {
        return 1;
    }

    g_client->WaitAllFinished();
    fprintf(stdout, "connection done\n");

    std::string line;
    std::cout << "Query: " << std::flush;
    while (std::getline(std::cin, line))
    {
        if (line.empty())
        {
            break;
        }

        SPTAG::Socket::RemoteQuery query;
        query.m_type = SPTAG::Socket::RemoteQuery::QueryType::String;
        query.m_queryString = std::move(line);

        SPTAG::Socket::RemoteSearchResult result;
        auto callback = [&result](SPTAG::Socket::RemoteSearchResult p_result)
        {
            result = std::move(p_result);
        };

        g_client->SendQueryAsync(query, callback, options);
        g_client->WaitAllFinished();

        std::cout << "Status: " << static_cast<std::uint32_t>(result.m_status) << std::endl;

        for (const auto& indexRes : result.m_allIndexResults)
        {
            std::cout << "Index: " << indexRes.m_indexName << std::endl;

            int idx = 0;
            for (const auto& res : indexRes.m_results)
            {
                std::cout << "------------------" << std::endl;
                std::cout << "DocIndex: " << res.VID << " Distance: " << res.Dist;
                if (indexRes.m_results.WithMeta())
                {
                    const auto& metadata = indexRes.m_results.GetMetadata(idx);
                    std::cout << " MetaData: " << std::string((char*)metadata.Data(), metadata.Length());
                }
                std::cout << std::endl;
                ++idx;
            }
        }

        std::cout << "Query: " << std::flush;
    }

    return 0;
}

