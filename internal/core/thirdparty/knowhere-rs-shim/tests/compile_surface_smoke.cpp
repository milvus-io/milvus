#include "knowhere/version.h"
#include "knowhere/dataset.h"
#include "knowhere/index_node.h"
#include "knowhere/index/index_factory.h"

int
main() {
    auto ds = knowhere::GenDataSet(1, 4, nullptr);
    (void)ds;
    auto version = knowhere::Version::GetCurrentVersion().VersionNumber();
    (void)version;
    return 0;
}
