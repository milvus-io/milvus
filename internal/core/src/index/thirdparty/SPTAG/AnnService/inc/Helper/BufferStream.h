// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_BUFFERSTREAM_H_
#define _SPTAG_HELPER_BUFFERSTREAM_H_

#include <streambuf>
#include <ostream>
#include <memory>

namespace SPTAG
{
    namespace Helper
    {
        struct streambuf : public std::basic_streambuf<char> 
        {
            streambuf(char* buffer, size_t size)
            {
                setp(buffer, buffer + size);
            }
        };

        class obufferstream : public std::ostream
        {
        public:
            obufferstream(streambuf* buf, bool transferOwnership) : std::ostream(buf)
            {
                if (transferOwnership)
                    m_bufHolder.reset(buf, std::default_delete<streambuf>());
            }

        private:
            std::shared_ptr<streambuf> m_bufHolder;
        };
    } // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_BUFFERSTREAM_H_

