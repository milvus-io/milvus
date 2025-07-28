mod cc_cedict;
mod common;
mod fetch;
mod ipadic;
mod ipadic_neologd;
mod ko_dic;
mod unidic;

use crate::error::Result;
use lindera::dictionary::{Dictionary, DictionaryKind};
pub fn load_dictionary_from_kind(
    kind: &DictionaryKind,
    build_dir: String,
    download_url: Vec<String>,
) -> Result<Dictionary> {
    match kind {
        DictionaryKind::IPADIC => ipadic::load_ipadic(build_dir, download_url),
        DictionaryKind::CcCedict => cc_cedict::load_cc_cedict(build_dir, download_url),
        DictionaryKind::KoDic => ko_dic::load_ko_dic(build_dir, download_url),
        DictionaryKind::IPADICNEologd => {
            ipadic_neologd::load_ipadic_neologd(build_dir, download_url)
        }
        DictionaryKind::UniDic => unidic::load_unidic(build_dir, download_url),
    }
}
