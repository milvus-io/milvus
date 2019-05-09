using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

#if (!NET45)
namespace Thrift
{
    static class StreamExtensionsNet35
    {
        // CopyTo() has been added in 4.0
        public static long CopyTo(this Stream source, Stream target)
        {
            byte[] buffer = new byte[8192];  // multiple of 4096
            long nTotal = 0;
            while (true)
            {
                int nRead = source.Read(buffer, 0, buffer.Length);
                if (nRead <= 0)  // done?
                    return nTotal;

                target.Write(buffer, 0, nRead);
                nTotal += nRead;
            }
        }
    }

}
#endif

