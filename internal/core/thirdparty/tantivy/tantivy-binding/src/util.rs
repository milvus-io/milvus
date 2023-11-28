use std::ops::Bound;

use tantivy::{directory::MmapDirectory, Index};

pub fn index_exist(path: &str) -> bool {
    let dir = MmapDirectory::open(path).unwrap();
    Index::exists(&dir).unwrap()
}

pub fn make_bounds<T>(bound: T, inclusive: bool) -> Bound<T> {
    if inclusive {
        Bound::Included(bound)
    } else {
        Bound::Excluded(bound)
    }
}
