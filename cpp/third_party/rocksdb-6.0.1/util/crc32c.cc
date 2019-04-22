//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A portable implementation of crc32c, optimized to handle
// four bytes at a time.
#include "util/crc32c.h"
#include <stdint.h>
#ifdef HAVE_SSE42
#include <nmmintrin.h>
#include <wmmintrin.h>
#endif
#include "util/coding.h"
#include "util/util.h"

#ifdef __powerpc64__
#include "util/crc32c_ppc.h"
#include "util/crc32c_ppc_constants.h"

#if __linux__
#include <sys/auxv.h>

#ifndef PPC_FEATURE2_VEC_CRYPTO
#define PPC_FEATURE2_VEC_CRYPTO 0x02000000
#endif

#ifndef AT_HWCAP2
#define AT_HWCAP2 26
#endif

#endif /* __linux__ */

#endif

namespace rocksdb {
namespace crc32c {

#if defined(HAVE_POWER8) && defined(HAS_ALTIVEC)
#ifdef __powerpc64__
static int arch_ppc_crc32 = 0;
#endif /* __powerpc64__ */
#endif

static const uint32_t table0_[256] = {
  0x00000000, 0xf26b8303, 0xe13b70f7, 0x1350f3f4,
  0xc79a971f, 0x35f1141c, 0x26a1e7e8, 0xd4ca64eb,
  0x8ad958cf, 0x78b2dbcc, 0x6be22838, 0x9989ab3b,
  0x4d43cfd0, 0xbf284cd3, 0xac78bf27, 0x5e133c24,
  0x105ec76f, 0xe235446c, 0xf165b798, 0x030e349b,
  0xd7c45070, 0x25afd373, 0x36ff2087, 0xc494a384,
  0x9a879fa0, 0x68ec1ca3, 0x7bbcef57, 0x89d76c54,
  0x5d1d08bf, 0xaf768bbc, 0xbc267848, 0x4e4dfb4b,
  0x20bd8ede, 0xd2d60ddd, 0xc186fe29, 0x33ed7d2a,
  0xe72719c1, 0x154c9ac2, 0x061c6936, 0xf477ea35,
  0xaa64d611, 0x580f5512, 0x4b5fa6e6, 0xb93425e5,
  0x6dfe410e, 0x9f95c20d, 0x8cc531f9, 0x7eaeb2fa,
  0x30e349b1, 0xc288cab2, 0xd1d83946, 0x23b3ba45,
  0xf779deae, 0x05125dad, 0x1642ae59, 0xe4292d5a,
  0xba3a117e, 0x4851927d, 0x5b016189, 0xa96ae28a,
  0x7da08661, 0x8fcb0562, 0x9c9bf696, 0x6ef07595,
  0x417b1dbc, 0xb3109ebf, 0xa0406d4b, 0x522bee48,
  0x86e18aa3, 0x748a09a0, 0x67dafa54, 0x95b17957,
  0xcba24573, 0x39c9c670, 0x2a993584, 0xd8f2b687,
  0x0c38d26c, 0xfe53516f, 0xed03a29b, 0x1f682198,
  0x5125dad3, 0xa34e59d0, 0xb01eaa24, 0x42752927,
  0x96bf4dcc, 0x64d4cecf, 0x77843d3b, 0x85efbe38,
  0xdbfc821c, 0x2997011f, 0x3ac7f2eb, 0xc8ac71e8,
  0x1c661503, 0xee0d9600, 0xfd5d65f4, 0x0f36e6f7,
  0x61c69362, 0x93ad1061, 0x80fde395, 0x72966096,
  0xa65c047d, 0x5437877e, 0x4767748a, 0xb50cf789,
  0xeb1fcbad, 0x197448ae, 0x0a24bb5a, 0xf84f3859,
  0x2c855cb2, 0xdeeedfb1, 0xcdbe2c45, 0x3fd5af46,
  0x7198540d, 0x83f3d70e, 0x90a324fa, 0x62c8a7f9,
  0xb602c312, 0x44694011, 0x5739b3e5, 0xa55230e6,
  0xfb410cc2, 0x092a8fc1, 0x1a7a7c35, 0xe811ff36,
  0x3cdb9bdd, 0xceb018de, 0xdde0eb2a, 0x2f8b6829,
  0x82f63b78, 0x709db87b, 0x63cd4b8f, 0x91a6c88c,
  0x456cac67, 0xb7072f64, 0xa457dc90, 0x563c5f93,
  0x082f63b7, 0xfa44e0b4, 0xe9141340, 0x1b7f9043,
  0xcfb5f4a8, 0x3dde77ab, 0x2e8e845f, 0xdce5075c,
  0x92a8fc17, 0x60c37f14, 0x73938ce0, 0x81f80fe3,
  0x55326b08, 0xa759e80b, 0xb4091bff, 0x466298fc,
  0x1871a4d8, 0xea1a27db, 0xf94ad42f, 0x0b21572c,
  0xdfeb33c7, 0x2d80b0c4, 0x3ed04330, 0xccbbc033,
  0xa24bb5a6, 0x502036a5, 0x4370c551, 0xb11b4652,
  0x65d122b9, 0x97baa1ba, 0x84ea524e, 0x7681d14d,
  0x2892ed69, 0xdaf96e6a, 0xc9a99d9e, 0x3bc21e9d,
  0xef087a76, 0x1d63f975, 0x0e330a81, 0xfc588982,
  0xb21572c9, 0x407ef1ca, 0x532e023e, 0xa145813d,
  0x758fe5d6, 0x87e466d5, 0x94b49521, 0x66df1622,
  0x38cc2a06, 0xcaa7a905, 0xd9f75af1, 0x2b9cd9f2,
  0xff56bd19, 0x0d3d3e1a, 0x1e6dcdee, 0xec064eed,
  0xc38d26c4, 0x31e6a5c7, 0x22b65633, 0xd0ddd530,
  0x0417b1db, 0xf67c32d8, 0xe52cc12c, 0x1747422f,
  0x49547e0b, 0xbb3ffd08, 0xa86f0efc, 0x5a048dff,
  0x8ecee914, 0x7ca56a17, 0x6ff599e3, 0x9d9e1ae0,
  0xd3d3e1ab, 0x21b862a8, 0x32e8915c, 0xc083125f,
  0x144976b4, 0xe622f5b7, 0xf5720643, 0x07198540,
  0x590ab964, 0xab613a67, 0xb831c993, 0x4a5a4a90,
  0x9e902e7b, 0x6cfbad78, 0x7fab5e8c, 0x8dc0dd8f,
  0xe330a81a, 0x115b2b19, 0x020bd8ed, 0xf0605bee,
  0x24aa3f05, 0xd6c1bc06, 0xc5914ff2, 0x37faccf1,
  0x69e9f0d5, 0x9b8273d6, 0x88d28022, 0x7ab90321,
  0xae7367ca, 0x5c18e4c9, 0x4f48173d, 0xbd23943e,
  0xf36e6f75, 0x0105ec76, 0x12551f82, 0xe03e9c81,
  0x34f4f86a, 0xc69f7b69, 0xd5cf889d, 0x27a40b9e,
  0x79b737ba, 0x8bdcb4b9, 0x988c474d, 0x6ae7c44e,
  0xbe2da0a5, 0x4c4623a6, 0x5f16d052, 0xad7d5351
};
static const uint32_t table1_[256] = {
  0x00000000, 0x13a29877, 0x274530ee, 0x34e7a899,
  0x4e8a61dc, 0x5d28f9ab, 0x69cf5132, 0x7a6dc945,
  0x9d14c3b8, 0x8eb65bcf, 0xba51f356, 0xa9f36b21,
  0xd39ea264, 0xc03c3a13, 0xf4db928a, 0xe7790afd,
  0x3fc5f181, 0x2c6769f6, 0x1880c16f, 0x0b225918,
  0x714f905d, 0x62ed082a, 0x560aa0b3, 0x45a838c4,
  0xa2d13239, 0xb173aa4e, 0x859402d7, 0x96369aa0,
  0xec5b53e5, 0xfff9cb92, 0xcb1e630b, 0xd8bcfb7c,
  0x7f8be302, 0x6c297b75, 0x58ced3ec, 0x4b6c4b9b,
  0x310182de, 0x22a31aa9, 0x1644b230, 0x05e62a47,
  0xe29f20ba, 0xf13db8cd, 0xc5da1054, 0xd6788823,
  0xac154166, 0xbfb7d911, 0x8b507188, 0x98f2e9ff,
  0x404e1283, 0x53ec8af4, 0x670b226d, 0x74a9ba1a,
  0x0ec4735f, 0x1d66eb28, 0x298143b1, 0x3a23dbc6,
  0xdd5ad13b, 0xcef8494c, 0xfa1fe1d5, 0xe9bd79a2,
  0x93d0b0e7, 0x80722890, 0xb4958009, 0xa737187e,
  0xff17c604, 0xecb55e73, 0xd852f6ea, 0xcbf06e9d,
  0xb19da7d8, 0xa23f3faf, 0x96d89736, 0x857a0f41,
  0x620305bc, 0x71a19dcb, 0x45463552, 0x56e4ad25,
  0x2c896460, 0x3f2bfc17, 0x0bcc548e, 0x186eccf9,
  0xc0d23785, 0xd370aff2, 0xe797076b, 0xf4359f1c,
  0x8e585659, 0x9dface2e, 0xa91d66b7, 0xbabffec0,
  0x5dc6f43d, 0x4e646c4a, 0x7a83c4d3, 0x69215ca4,
  0x134c95e1, 0x00ee0d96, 0x3409a50f, 0x27ab3d78,
  0x809c2506, 0x933ebd71, 0xa7d915e8, 0xb47b8d9f,
  0xce1644da, 0xddb4dcad, 0xe9537434, 0xfaf1ec43,
  0x1d88e6be, 0x0e2a7ec9, 0x3acdd650, 0x296f4e27,
  0x53028762, 0x40a01f15, 0x7447b78c, 0x67e52ffb,
  0xbf59d487, 0xacfb4cf0, 0x981ce469, 0x8bbe7c1e,
  0xf1d3b55b, 0xe2712d2c, 0xd69685b5, 0xc5341dc2,
  0x224d173f, 0x31ef8f48, 0x050827d1, 0x16aabfa6,
  0x6cc776e3, 0x7f65ee94, 0x4b82460d, 0x5820de7a,
  0xfbc3faf9, 0xe861628e, 0xdc86ca17, 0xcf245260,
  0xb5499b25, 0xa6eb0352, 0x920cabcb, 0x81ae33bc,
  0x66d73941, 0x7575a136, 0x419209af, 0x523091d8,
  0x285d589d, 0x3bffc0ea, 0x0f186873, 0x1cbaf004,
  0xc4060b78, 0xd7a4930f, 0xe3433b96, 0xf0e1a3e1,
  0x8a8c6aa4, 0x992ef2d3, 0xadc95a4a, 0xbe6bc23d,
  0x5912c8c0, 0x4ab050b7, 0x7e57f82e, 0x6df56059,
  0x1798a91c, 0x043a316b, 0x30dd99f2, 0x237f0185,
  0x844819fb, 0x97ea818c, 0xa30d2915, 0xb0afb162,
  0xcac27827, 0xd960e050, 0xed8748c9, 0xfe25d0be,
  0x195cda43, 0x0afe4234, 0x3e19eaad, 0x2dbb72da,
  0x57d6bb9f, 0x447423e8, 0x70938b71, 0x63311306,
  0xbb8de87a, 0xa82f700d, 0x9cc8d894, 0x8f6a40e3,
  0xf50789a6, 0xe6a511d1, 0xd242b948, 0xc1e0213f,
  0x26992bc2, 0x353bb3b5, 0x01dc1b2c, 0x127e835b,
  0x68134a1e, 0x7bb1d269, 0x4f567af0, 0x5cf4e287,
  0x04d43cfd, 0x1776a48a, 0x23910c13, 0x30339464,
  0x4a5e5d21, 0x59fcc556, 0x6d1b6dcf, 0x7eb9f5b8,
  0x99c0ff45, 0x8a626732, 0xbe85cfab, 0xad2757dc,
  0xd74a9e99, 0xc4e806ee, 0xf00fae77, 0xe3ad3600,
  0x3b11cd7c, 0x28b3550b, 0x1c54fd92, 0x0ff665e5,
  0x759baca0, 0x663934d7, 0x52de9c4e, 0x417c0439,
  0xa6050ec4, 0xb5a796b3, 0x81403e2a, 0x92e2a65d,
  0xe88f6f18, 0xfb2df76f, 0xcfca5ff6, 0xdc68c781,
  0x7b5fdfff, 0x68fd4788, 0x5c1aef11, 0x4fb87766,
  0x35d5be23, 0x26772654, 0x12908ecd, 0x013216ba,
  0xe64b1c47, 0xf5e98430, 0xc10e2ca9, 0xd2acb4de,
  0xa8c17d9b, 0xbb63e5ec, 0x8f844d75, 0x9c26d502,
  0x449a2e7e, 0x5738b609, 0x63df1e90, 0x707d86e7,
  0x0a104fa2, 0x19b2d7d5, 0x2d557f4c, 0x3ef7e73b,
  0xd98eedc6, 0xca2c75b1, 0xfecbdd28, 0xed69455f,
  0x97048c1a, 0x84a6146d, 0xb041bcf4, 0xa3e32483
};
static const uint32_t table2_[256] = {
  0x00000000, 0xa541927e, 0x4f6f520d, 0xea2ec073,
  0x9edea41a, 0x3b9f3664, 0xd1b1f617, 0x74f06469,
  0x38513ec5, 0x9d10acbb, 0x773e6cc8, 0xd27ffeb6,
  0xa68f9adf, 0x03ce08a1, 0xe9e0c8d2, 0x4ca15aac,
  0x70a27d8a, 0xd5e3eff4, 0x3fcd2f87, 0x9a8cbdf9,
  0xee7cd990, 0x4b3d4bee, 0xa1138b9d, 0x045219e3,
  0x48f3434f, 0xedb2d131, 0x079c1142, 0xa2dd833c,
  0xd62de755, 0x736c752b, 0x9942b558, 0x3c032726,
  0xe144fb14, 0x4405696a, 0xae2ba919, 0x0b6a3b67,
  0x7f9a5f0e, 0xdadbcd70, 0x30f50d03, 0x95b49f7d,
  0xd915c5d1, 0x7c5457af, 0x967a97dc, 0x333b05a2,
  0x47cb61cb, 0xe28af3b5, 0x08a433c6, 0xade5a1b8,
  0x91e6869e, 0x34a714e0, 0xde89d493, 0x7bc846ed,
  0x0f382284, 0xaa79b0fa, 0x40577089, 0xe516e2f7,
  0xa9b7b85b, 0x0cf62a25, 0xe6d8ea56, 0x43997828,
  0x37691c41, 0x92288e3f, 0x78064e4c, 0xdd47dc32,
  0xc76580d9, 0x622412a7, 0x880ad2d4, 0x2d4b40aa,
  0x59bb24c3, 0xfcfab6bd, 0x16d476ce, 0xb395e4b0,
  0xff34be1c, 0x5a752c62, 0xb05bec11, 0x151a7e6f,
  0x61ea1a06, 0xc4ab8878, 0x2e85480b, 0x8bc4da75,
  0xb7c7fd53, 0x12866f2d, 0xf8a8af5e, 0x5de93d20,
  0x29195949, 0x8c58cb37, 0x66760b44, 0xc337993a,
  0x8f96c396, 0x2ad751e8, 0xc0f9919b, 0x65b803e5,
  0x1148678c, 0xb409f5f2, 0x5e273581, 0xfb66a7ff,
  0x26217bcd, 0x8360e9b3, 0x694e29c0, 0xcc0fbbbe,
  0xb8ffdfd7, 0x1dbe4da9, 0xf7908dda, 0x52d11fa4,
  0x1e704508, 0xbb31d776, 0x511f1705, 0xf45e857b,
  0x80aee112, 0x25ef736c, 0xcfc1b31f, 0x6a802161,
  0x56830647, 0xf3c29439, 0x19ec544a, 0xbcadc634,
  0xc85da25d, 0x6d1c3023, 0x8732f050, 0x2273622e,
  0x6ed23882, 0xcb93aafc, 0x21bd6a8f, 0x84fcf8f1,
  0xf00c9c98, 0x554d0ee6, 0xbf63ce95, 0x1a225ceb,
  0x8b277743, 0x2e66e53d, 0xc448254e, 0x6109b730,
  0x15f9d359, 0xb0b84127, 0x5a968154, 0xffd7132a,
  0xb3764986, 0x1637dbf8, 0xfc191b8b, 0x595889f5,
  0x2da8ed9c, 0x88e97fe2, 0x62c7bf91, 0xc7862def,
  0xfb850ac9, 0x5ec498b7, 0xb4ea58c4, 0x11abcaba,
  0x655baed3, 0xc01a3cad, 0x2a34fcde, 0x8f756ea0,
  0xc3d4340c, 0x6695a672, 0x8cbb6601, 0x29faf47f,
  0x5d0a9016, 0xf84b0268, 0x1265c21b, 0xb7245065,
  0x6a638c57, 0xcf221e29, 0x250cde5a, 0x804d4c24,
  0xf4bd284d, 0x51fcba33, 0xbbd27a40, 0x1e93e83e,
  0x5232b292, 0xf77320ec, 0x1d5de09f, 0xb81c72e1,
  0xccec1688, 0x69ad84f6, 0x83834485, 0x26c2d6fb,
  0x1ac1f1dd, 0xbf8063a3, 0x55aea3d0, 0xf0ef31ae,
  0x841f55c7, 0x215ec7b9, 0xcb7007ca, 0x6e3195b4,
  0x2290cf18, 0x87d15d66, 0x6dff9d15, 0xc8be0f6b,
  0xbc4e6b02, 0x190ff97c, 0xf321390f, 0x5660ab71,
  0x4c42f79a, 0xe90365e4, 0x032da597, 0xa66c37e9,
  0xd29c5380, 0x77ddc1fe, 0x9df3018d, 0x38b293f3,
  0x7413c95f, 0xd1525b21, 0x3b7c9b52, 0x9e3d092c,
  0xeacd6d45, 0x4f8cff3b, 0xa5a23f48, 0x00e3ad36,
  0x3ce08a10, 0x99a1186e, 0x738fd81d, 0xd6ce4a63,
  0xa23e2e0a, 0x077fbc74, 0xed517c07, 0x4810ee79,
  0x04b1b4d5, 0xa1f026ab, 0x4bdee6d8, 0xee9f74a6,
  0x9a6f10cf, 0x3f2e82b1, 0xd50042c2, 0x7041d0bc,
  0xad060c8e, 0x08479ef0, 0xe2695e83, 0x4728ccfd,
  0x33d8a894, 0x96993aea, 0x7cb7fa99, 0xd9f668e7,
  0x9557324b, 0x3016a035, 0xda386046, 0x7f79f238,
  0x0b899651, 0xaec8042f, 0x44e6c45c, 0xe1a75622,
  0xdda47104, 0x78e5e37a, 0x92cb2309, 0x378ab177,
  0x437ad51e, 0xe63b4760, 0x0c158713, 0xa954156d,
  0xe5f54fc1, 0x40b4ddbf, 0xaa9a1dcc, 0x0fdb8fb2,
  0x7b2bebdb, 0xde6a79a5, 0x3444b9d6, 0x91052ba8
};
static const uint32_t table3_[256] = {
  0x00000000, 0xdd45aab8, 0xbf672381, 0x62228939,
  0x7b2231f3, 0xa6679b4b, 0xc4451272, 0x1900b8ca,
  0xf64463e6, 0x2b01c95e, 0x49234067, 0x9466eadf,
  0x8d665215, 0x5023f8ad, 0x32017194, 0xef44db2c,
  0xe964b13d, 0x34211b85, 0x560392bc, 0x8b463804,
  0x924680ce, 0x4f032a76, 0x2d21a34f, 0xf06409f7,
  0x1f20d2db, 0xc2657863, 0xa047f15a, 0x7d025be2,
  0x6402e328, 0xb9474990, 0xdb65c0a9, 0x06206a11,
  0xd725148b, 0x0a60be33, 0x6842370a, 0xb5079db2,
  0xac072578, 0x71428fc0, 0x136006f9, 0xce25ac41,
  0x2161776d, 0xfc24ddd5, 0x9e0654ec, 0x4343fe54,
  0x5a43469e, 0x8706ec26, 0xe524651f, 0x3861cfa7,
  0x3e41a5b6, 0xe3040f0e, 0x81268637, 0x5c632c8f,
  0x45639445, 0x98263efd, 0xfa04b7c4, 0x27411d7c,
  0xc805c650, 0x15406ce8, 0x7762e5d1, 0xaa274f69,
  0xb327f7a3, 0x6e625d1b, 0x0c40d422, 0xd1057e9a,
  0xaba65fe7, 0x76e3f55f, 0x14c17c66, 0xc984d6de,
  0xd0846e14, 0x0dc1c4ac, 0x6fe34d95, 0xb2a6e72d,
  0x5de23c01, 0x80a796b9, 0xe2851f80, 0x3fc0b538,
  0x26c00df2, 0xfb85a74a, 0x99a72e73, 0x44e284cb,
  0x42c2eeda, 0x9f874462, 0xfda5cd5b, 0x20e067e3,
  0x39e0df29, 0xe4a57591, 0x8687fca8, 0x5bc25610,
  0xb4868d3c, 0x69c32784, 0x0be1aebd, 0xd6a40405,
  0xcfa4bccf, 0x12e11677, 0x70c39f4e, 0xad8635f6,
  0x7c834b6c, 0xa1c6e1d4, 0xc3e468ed, 0x1ea1c255,
  0x07a17a9f, 0xdae4d027, 0xb8c6591e, 0x6583f3a6,
  0x8ac7288a, 0x57828232, 0x35a00b0b, 0xe8e5a1b3,
  0xf1e51979, 0x2ca0b3c1, 0x4e823af8, 0x93c79040,
  0x95e7fa51, 0x48a250e9, 0x2a80d9d0, 0xf7c57368,
  0xeec5cba2, 0x3380611a, 0x51a2e823, 0x8ce7429b,
  0x63a399b7, 0xbee6330f, 0xdcc4ba36, 0x0181108e,
  0x1881a844, 0xc5c402fc, 0xa7e68bc5, 0x7aa3217d,
  0x52a0c93f, 0x8fe56387, 0xedc7eabe, 0x30824006,
  0x2982f8cc, 0xf4c75274, 0x96e5db4d, 0x4ba071f5,
  0xa4e4aad9, 0x79a10061, 0x1b838958, 0xc6c623e0,
  0xdfc69b2a, 0x02833192, 0x60a1b8ab, 0xbde41213,
  0xbbc47802, 0x6681d2ba, 0x04a35b83, 0xd9e6f13b,
  0xc0e649f1, 0x1da3e349, 0x7f816a70, 0xa2c4c0c8,
  0x4d801be4, 0x90c5b15c, 0xf2e73865, 0x2fa292dd,
  0x36a22a17, 0xebe780af, 0x89c50996, 0x5480a32e,
  0x8585ddb4, 0x58c0770c, 0x3ae2fe35, 0xe7a7548d,
  0xfea7ec47, 0x23e246ff, 0x41c0cfc6, 0x9c85657e,
  0x73c1be52, 0xae8414ea, 0xcca69dd3, 0x11e3376b,
  0x08e38fa1, 0xd5a62519, 0xb784ac20, 0x6ac10698,
  0x6ce16c89, 0xb1a4c631, 0xd3864f08, 0x0ec3e5b0,
  0x17c35d7a, 0xca86f7c2, 0xa8a47efb, 0x75e1d443,
  0x9aa50f6f, 0x47e0a5d7, 0x25c22cee, 0xf8878656,
  0xe1873e9c, 0x3cc29424, 0x5ee01d1d, 0x83a5b7a5,
  0xf90696d8, 0x24433c60, 0x4661b559, 0x9b241fe1,
  0x8224a72b, 0x5f610d93, 0x3d4384aa, 0xe0062e12,
  0x0f42f53e, 0xd2075f86, 0xb025d6bf, 0x6d607c07,
  0x7460c4cd, 0xa9256e75, 0xcb07e74c, 0x16424df4,
  0x106227e5, 0xcd278d5d, 0xaf050464, 0x7240aedc,
  0x6b401616, 0xb605bcae, 0xd4273597, 0x09629f2f,
  0xe6264403, 0x3b63eebb, 0x59416782, 0x8404cd3a,
  0x9d0475f0, 0x4041df48, 0x22635671, 0xff26fcc9,
  0x2e238253, 0xf36628eb, 0x9144a1d2, 0x4c010b6a,
  0x5501b3a0, 0x88441918, 0xea669021, 0x37233a99,
  0xd867e1b5, 0x05224b0d, 0x6700c234, 0xba45688c,
  0xa345d046, 0x7e007afe, 0x1c22f3c7, 0xc167597f,
  0xc747336e, 0x1a0299d6, 0x782010ef, 0xa565ba57,
  0xbc65029d, 0x6120a825, 0x0302211c, 0xde478ba4,
  0x31035088, 0xec46fa30, 0x8e647309, 0x5321d9b1,
  0x4a21617b, 0x9764cbc3, 0xf54642fa, 0x2803e842
};

// Used to fetch a naturally-aligned 32-bit word in little endian byte-order
static inline uint32_t LE_LOAD32(const uint8_t *p) {
  return DecodeFixed32(reinterpret_cast<const char*>(p));
}

#if defined(HAVE_SSE42) && (defined(__LP64__) || defined(_WIN64))
static inline uint64_t LE_LOAD64(const uint8_t *p) {
  return DecodeFixed64(reinterpret_cast<const char*>(p));
}
#endif

static inline void Slow_CRC32(uint64_t* l, uint8_t const **p) {
  uint32_t c = static_cast<uint32_t>(*l ^ LE_LOAD32(*p));
  *p += 4;
  *l = table3_[c & 0xff] ^
  table2_[(c >> 8) & 0xff] ^
  table1_[(c >> 16) & 0xff] ^
  table0_[c >> 24];
  // DO it twice.
  c = static_cast<uint32_t>(*l ^ LE_LOAD32(*p));
  *p += 4;
  *l = table3_[c & 0xff] ^
  table2_[(c >> 8) & 0xff] ^
  table1_[(c >> 16) & 0xff] ^
  table0_[c >> 24];
}

static inline void Fast_CRC32(uint64_t* l, uint8_t const **p) {
#ifndef HAVE_SSE42
  Slow_CRC32(l, p);
#elif defined(__LP64__) || defined(_WIN64)
  *l = _mm_crc32_u64(*l, LE_LOAD64(*p));
  *p += 8;
#else
  *l = _mm_crc32_u32(static_cast<unsigned int>(*l), LE_LOAD32(*p));
  *p += 4;
  *l = _mm_crc32_u32(static_cast<unsigned int>(*l), LE_LOAD32(*p));
  *p += 4;
#endif
}

template<void (*CRC32)(uint64_t*, uint8_t const**)>
uint32_t ExtendImpl(uint32_t crc, const char* buf, size_t size) {

  const uint8_t *p = reinterpret_cast<const uint8_t *>(buf);
  const uint8_t *e = p + size;
  uint64_t l = crc ^ 0xffffffffu;

// Align n to (1 << m) byte boundary
#define ALIGN(n, m)     ((n + ((1 << m) - 1)) & ~((1 << m) - 1))

#define STEP1 do {                              \
    int c = (l & 0xff) ^ *p++;                  \
    l = table0_[c] ^ (l >> 8);                  \
} while (0)


  // Point x at first 16-byte aligned byte in string.  This might be
  // just past the end of the string.
  const uintptr_t pval = reinterpret_cast<uintptr_t>(p);
  const uint8_t* x = reinterpret_cast<const uint8_t*>(ALIGN(pval, 4));
  if (x <= e) {
    // Process bytes until finished or p is 16-byte aligned
    while (p != x) {
      STEP1;
    }
  }
  // Process bytes 16 at a time
  while ((e-p) >= 16) {
    CRC32(&l, &p);
    CRC32(&l, &p);
  }
  // Process bytes 8 at a time
  while ((e-p) >= 8) {
    CRC32(&l, &p);
  }
  // Process the last few bytes
  while (p != e) {
    STEP1;
  }
#undef STEP1
#undef ALIGN
  return static_cast<uint32_t>(l ^ 0xffffffffu);
}

// Detect if SS42 or not.
#ifndef HAVE_POWER8

static bool isSSE42() {
#ifndef HAVE_SSE42
  return false;
#elif defined(__GNUC__) && defined(__x86_64__) && !defined(IOS_CROSS_COMPILE)
  uint32_t c_;
  __asm__("cpuid" : "=c"(c_) : "a"(1) : "ebx", "edx");
  return c_ & (1U << 20);  // copied from CpuId.h in Folly. Test SSE42
#elif defined(_WIN64)
  int info[4];
  __cpuidex(info, 0x00000001, 0);
  return (info[2] & ((int)1 << 20)) != 0;
#else
  return false;
#endif
}

static bool isPCLMULQDQ() {
#ifndef HAVE_SSE42
// in build_detect_platform we set this macro when both SSE42 and PCLMULQDQ are
// supported by compiler
  return false;
#elif defined(__GNUC__) && defined(__x86_64__) && !defined(IOS_CROSS_COMPILE)
  uint32_t c_;
  __asm__("cpuid" : "=c"(c_) : "a"(1) : "ebx", "edx");
  return c_ & (1U << 1);  // PCLMULQDQ is in bit 1 (not bit 0)
#elif defined(_WIN64)
  int info[4];
  __cpuidex(info, 0x00000001, 0);
  return (info[2] & ((int)1 << 1)) != 0;
#else
  return false;
#endif
}

#endif  // HAVE_POWER8

typedef uint32_t (*Function)(uint32_t, const char*, size_t);

#if defined(HAVE_POWER8) && defined(HAS_ALTIVEC)
uint32_t ExtendPPCImpl(uint32_t crc, const char *buf, size_t size) {
  return crc32c_ppc(crc, (const unsigned char *)buf, size);
}

#if __linux__
static int arch_ppc_probe(void) {
  arch_ppc_crc32 = 0;

#if defined(__powerpc64__)
  if (getauxval(AT_HWCAP2) & PPC_FEATURE2_VEC_CRYPTO) arch_ppc_crc32 = 1;
#endif /* __powerpc64__ */

  return arch_ppc_crc32;
}
#endif  // __linux__

static bool isAltiVec() {
  if (arch_ppc_probe()) {
    return true;
  } else {
    return false;
  }
}
#endif


std::string IsFastCrc32Supported() {
  bool has_fast_crc = false;
  std::string fast_zero_msg;
  std::string arch;
#ifdef HAVE_POWER8
#ifdef HAS_ALTIVEC
  if (arch_ppc_probe()) {
    has_fast_crc = true;
    arch = "PPC";
  }
#else
  has_fast_crc = false;
  arch = "PPC";
#endif
#else
  has_fast_crc = isSSE42();
  arch = "x86";
#endif
  if (has_fast_crc) {
    fast_zero_msg.append("Supported on " + arch);
  }
  else {
    fast_zero_msg.append("Not supported on " + arch);
  }
  return fast_zero_msg;
}


/*
 * Copyright 2016 Ferry Toth, Exalon Delft BV, The Netherlands
 *  This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the author be held liable for any damages
 * arising from the use of this software.
 *  Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *  1. The origin of this software must not be misrepresented; you must not
 *   claim that you wrote the original software. If you use this software
 *   in a product, an acknowledgment in the product documentation would be
 *   appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *   misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 *  Ferry Toth
 * ftoth@exalondelft.nl
 *
 * https://github.com/htot/crc32c
 *
 * Modified by Facebook
 *
 * Original intel whitepaper:
 * "Fast CRC Computation for iSCSI Polynomial Using CRC32 Instruction"
 * https://www.intel.com/content/dam/www/public/us/en/documents/white-papers/crc-iscsi-polynomial-crc32-instruction-paper.pdf
 *
 * This version is from the folly library, created by Dave Watson <davejwatson@fb.com>
 *
*/
#if defined HAVE_SSE42 && defined HAVE_PCLMUL

#define CRCtriplet(crc, buf, offset)                  \
  crc##0 = _mm_crc32_u64(crc##0, *(buf##0 + offset)); \
  crc##1 = _mm_crc32_u64(crc##1, *(buf##1 + offset)); \
  crc##2 = _mm_crc32_u64(crc##2, *(buf##2 + offset));

#define CRCduplet(crc, buf, offset)                   \
  crc##0 = _mm_crc32_u64(crc##0, *(buf##0 + offset)); \
  crc##1 = _mm_crc32_u64(crc##1, *(buf##1 + offset));

#define CRCsinglet(crc, buf, offset)                    \
  crc = _mm_crc32_u64(crc, *(uint64_t*)(buf + offset));


// Numbers taken directly from intel whitepaper.
// clang-format off
const uint64_t clmul_constants[] = {
    0x14cd00bd6, 0x105ec76f0, 0x0ba4fc28e, 0x14cd00bd6,
    0x1d82c63da, 0x0f20c0dfe, 0x09e4addf8, 0x0ba4fc28e,
    0x039d3b296, 0x1384aa63a, 0x102f9b8a2, 0x1d82c63da,
    0x14237f5e6, 0x01c291d04, 0x00d3b6092, 0x09e4addf8,
    0x0c96cfdc0, 0x0740eef02, 0x18266e456, 0x039d3b296,
    0x0daece73e, 0x0083a6eec, 0x0ab7aff2a, 0x102f9b8a2,
    0x1248ea574, 0x1c1733996, 0x083348832, 0x14237f5e6,
    0x12c743124, 0x02ad91c30, 0x0b9e02b86, 0x00d3b6092,
    0x018b33a4e, 0x06992cea2, 0x1b331e26a, 0x0c96cfdc0,
    0x17d35ba46, 0x07e908048, 0x1bf2e8b8a, 0x18266e456,
    0x1a3e0968a, 0x11ed1f9d8, 0x0ce7f39f4, 0x0daece73e,
    0x061d82e56, 0x0f1d0f55e, 0x0d270f1a2, 0x0ab7aff2a,
    0x1c3f5f66c, 0x0a87ab8a8, 0x12ed0daac, 0x1248ea574,
    0x065863b64, 0x08462d800, 0x11eef4f8e, 0x083348832,
    0x1ee54f54c, 0x071d111a8, 0x0b3e32c28, 0x12c743124,
    0x0064f7f26, 0x0ffd852c6, 0x0dd7e3b0c, 0x0b9e02b86,
    0x0f285651c, 0x0dcb17aa4, 0x010746f3c, 0x018b33a4e,
    0x1c24afea4, 0x0f37c5aee, 0x0271d9844, 0x1b331e26a,
    0x08e766a0c, 0x06051d5a2, 0x093a5f730, 0x17d35ba46,
    0x06cb08e5c, 0x11d5ca20e, 0x06b749fb2, 0x1bf2e8b8a,
    0x1167f94f2, 0x021f3d99c, 0x0cec3662e, 0x1a3e0968a,
    0x19329634a, 0x08f158014, 0x0e6fc4e6a, 0x0ce7f39f4,
    0x08227bb8a, 0x1a5e82106, 0x0b0cd4768, 0x061d82e56,
    0x13c2b89c4, 0x188815ab2, 0x0d7a4825c, 0x0d270f1a2,
    0x10f5ff2ba, 0x105405f3e, 0x00167d312, 0x1c3f5f66c,
    0x0f6076544, 0x0e9adf796, 0x026f6a60a, 0x12ed0daac,
    0x1a2adb74e, 0x096638b34, 0x19d34af3a, 0x065863b64,
    0x049c3cc9c, 0x1e50585a0, 0x068bce87a, 0x11eef4f8e,
    0x1524fa6c6, 0x19f1c69dc, 0x16cba8aca, 0x1ee54f54c,
    0x042d98888, 0x12913343e, 0x1329d9f7e, 0x0b3e32c28,
    0x1b1c69528, 0x088f25a3a, 0x02178513a, 0x0064f7f26,
    0x0e0ac139e, 0x04e36f0b0, 0x0170076fa, 0x0dd7e3b0c,
    0x141a1a2e2, 0x0bd6f81f8, 0x16ad828b4, 0x0f285651c,
    0x041d17b64, 0x19425cbba, 0x1fae1cc66, 0x010746f3c,
    0x1a75b4b00, 0x18db37e8a, 0x0f872e54c, 0x1c24afea4,
    0x01e41e9fc, 0x04c144932, 0x086d8e4d2, 0x0271d9844,
    0x160f7af7a, 0x052148f02, 0x05bb8f1bc, 0x08e766a0c,
    0x0a90fd27a, 0x0a3c6f37a, 0x0b3af077a, 0x093a5f730,
    0x04984d782, 0x1d22c238e, 0x0ca6ef3ac, 0x06cb08e5c,
    0x0234e0b26, 0x063ded06a, 0x1d88abd4a, 0x06b749fb2,
    0x04597456a, 0x04d56973c, 0x0e9e28eb4, 0x1167f94f2,
    0x07b3ff57a, 0x19385bf2e, 0x0c9c8b782, 0x0cec3662e,
    0x13a9cba9e, 0x0e417f38a, 0x093e106a4, 0x19329634a,
    0x167001a9c, 0x14e727980, 0x1ddffc5d4, 0x0e6fc4e6a,
    0x00df04680, 0x0d104b8fc, 0x02342001e, 0x08227bb8a,
    0x00a2a8d7e, 0x05b397730, 0x168763fa6, 0x0b0cd4768,
    0x1ed5a407a, 0x0e78eb416, 0x0d2c3ed1a, 0x13c2b89c4,
    0x0995a5724, 0x1641378f0, 0x19b1afbc4, 0x0d7a4825c,
    0x109ffedc0, 0x08d96551c, 0x0f2271e60, 0x10f5ff2ba,
    0x00b0bf8ca, 0x00bf80dd2, 0x123888b7a, 0x00167d312,
    0x1e888f7dc, 0x18dcddd1c, 0x002ee03b2, 0x0f6076544,
    0x183e8d8fe, 0x06a45d2b2, 0x133d7a042, 0x026f6a60a,
    0x116b0f50c, 0x1dd3e10e8, 0x05fabe670, 0x1a2adb74e,
    0x130004488, 0x0de87806c, 0x000bcf5f6, 0x19d34af3a,
    0x18f0c7078, 0x014338754, 0x017f27698, 0x049c3cc9c,
    0x058ca5f00, 0x15e3e77ee, 0x1af900c24, 0x068bce87a,
    0x0b5cfca28, 0x0dd07448e, 0x0ded288f8, 0x1524fa6c6,
    0x059f229bc, 0x1d8048348, 0x06d390dec, 0x16cba8aca,
    0x037170390, 0x0a3e3e02c, 0x06353c1cc, 0x042d98888,
    0x0c4584f5c, 0x0d73c7bea, 0x1f16a3418, 0x1329d9f7e,
    0x0531377e2, 0x185137662, 0x1d8d9ca7c, 0x1b1c69528,
    0x0b25b29f2, 0x18a08b5bc, 0x19fb2a8b0, 0x02178513a,
    0x1a08fe6ac, 0x1da758ae0, 0x045cddf4e, 0x0e0ac139e,
    0x1a91647f2, 0x169cf9eb0, 0x1a0f717c4, 0x0170076fa,
};

// Compute the crc32c value for buffer smaller than 8
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
inline void align_to_8(
    size_t len,
    uint64_t& crc0, // crc so far, updated on return
    const unsigned char*& next) { // next data pointer, updated on return
  uint32_t crc32bit = static_cast<uint32_t>(crc0);
  if (len & 0x04) {
    crc32bit = _mm_crc32_u32(crc32bit, *(uint32_t*)next);
    next += sizeof(uint32_t);
  }
  if (len & 0x02) {
    crc32bit = _mm_crc32_u16(crc32bit, *(uint16_t*)next);
    next += sizeof(uint16_t);
  }
  if (len & 0x01) {
    crc32bit = _mm_crc32_u8(crc32bit, *(next));
    next++;
  }
  crc0 = crc32bit;
}

//
// CombineCRC performs pclmulqdq multiplication of 2 partial CRC's and a well
// chosen constant and xor's these with the remaining CRC.
//
inline uint64_t CombineCRC(
    size_t block_size,
    uint64_t crc0,
    uint64_t crc1,
    uint64_t crc2,
    const uint64_t* next2) {
  const auto multiplier =
      *(reinterpret_cast<const __m128i*>(clmul_constants) + block_size - 1);
  const auto crc0_xmm = _mm_set_epi64x(0, crc0);
  const auto res0 = _mm_clmulepi64_si128(crc0_xmm, multiplier, 0x00);
  const auto crc1_xmm = _mm_set_epi64x(0, crc1);
  const auto res1 = _mm_clmulepi64_si128(crc1_xmm, multiplier, 0x10);
  const auto res = _mm_xor_si128(res0, res1);
  crc0 = _mm_cvtsi128_si64(res);
  crc0 = crc0 ^ *((uint64_t*)next2 - 1);
  crc2 = _mm_crc32_u64(crc2, crc0);
  return crc2;
}

// Compute CRC-32C using the Intel hardware instruction.
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
uint32_t crc32c_3way(uint32_t crc, const char* buf, size_t len) {
  const unsigned char* next = (const unsigned char*)buf;
  uint64_t count;
  uint64_t crc0, crc1, crc2;
  crc0 = crc ^ 0xffffffffu;


  if (len >= 8) {
    // if len > 216 then align and use triplets
    if (len > 216) {
      {
        // Work on the bytes (< 8) before the first 8-byte alignment addr starts
        uint64_t align_bytes = (8 - (uintptr_t)next) & 7;
        len -= align_bytes;
        align_to_8(align_bytes, crc0, next);
      }

      // Now work on the remaining blocks
      count = len / 24; // number of triplets
      len %= 24; // bytes remaining
      uint64_t n = count >> 7; // #blocks = first block + full blocks
      uint64_t block_size = count & 127;
      if (block_size == 0) {
        block_size = 128;
      } else {
        n++;
      }
      // points to the first byte of the next block
      const uint64_t* next0 = (uint64_t*)next + block_size;
      const uint64_t* next1 = next0 + block_size;
      const uint64_t* next2 = next1 + block_size;

      crc1 = crc2 = 0;
      // Use Duff's device, a for() loop inside a switch()
      // statement. This needs to execute at least once, round len
      // down to nearest triplet multiple
      switch (block_size) {
        case 128:
          do {
            // jumps here for a full block of len 128
            CRCtriplet(crc, next, -128);
	    FALLTHROUGH_INTENDED;
            case 127:
              // jumps here or below for the first block smaller
              CRCtriplet(crc, next, -127);
	      FALLTHROUGH_INTENDED;
            case 126:
              CRCtriplet(crc, next, -126); // than 128
	      FALLTHROUGH_INTENDED;
            case 125:
              CRCtriplet(crc, next, -125);
	      FALLTHROUGH_INTENDED;
            case 124:
              CRCtriplet(crc, next, -124);
	      FALLTHROUGH_INTENDED;
            case 123:
              CRCtriplet(crc, next, -123);
	      FALLTHROUGH_INTENDED;
            case 122:
              CRCtriplet(crc, next, -122);
	      FALLTHROUGH_INTENDED;
            case 121:
              CRCtriplet(crc, next, -121);
	      FALLTHROUGH_INTENDED;
            case 120:
              CRCtriplet(crc, next, -120);
              FALLTHROUGH_INTENDED;
            case 119:
              CRCtriplet(crc, next, -119);
              FALLTHROUGH_INTENDED;
            case 118:
              CRCtriplet(crc, next, -118);
              FALLTHROUGH_INTENDED;
            case 117:
              CRCtriplet(crc, next, -117);
              FALLTHROUGH_INTENDED;
            case 116:
              CRCtriplet(crc, next, -116);
              FALLTHROUGH_INTENDED;
            case 115:
              CRCtriplet(crc, next, -115);
              FALLTHROUGH_INTENDED;
            case 114:
              CRCtriplet(crc, next, -114);
              FALLTHROUGH_INTENDED;
            case 113:
              CRCtriplet(crc, next, -113);
              FALLTHROUGH_INTENDED;
            case 112:
              CRCtriplet(crc, next, -112);
              FALLTHROUGH_INTENDED;
            case 111:
              CRCtriplet(crc, next, -111);
              FALLTHROUGH_INTENDED;
            case 110:
              CRCtriplet(crc, next, -110);
              FALLTHROUGH_INTENDED;
            case 109:
              CRCtriplet(crc, next, -109);
              FALLTHROUGH_INTENDED;
            case 108:
              CRCtriplet(crc, next, -108);
              FALLTHROUGH_INTENDED;
            case 107:
              CRCtriplet(crc, next, -107);
              FALLTHROUGH_INTENDED;
            case 106:
              CRCtriplet(crc, next, -106);
              FALLTHROUGH_INTENDED;
            case 105:
              CRCtriplet(crc, next, -105);
              FALLTHROUGH_INTENDED;
            case 104:
              CRCtriplet(crc, next, -104);
              FALLTHROUGH_INTENDED;
            case 103:
              CRCtriplet(crc, next, -103);
              FALLTHROUGH_INTENDED;
            case 102:
              CRCtriplet(crc, next, -102);
              FALLTHROUGH_INTENDED;
            case 101:
              CRCtriplet(crc, next, -101);
              FALLTHROUGH_INTENDED;
            case 100:
              CRCtriplet(crc, next, -100);
              FALLTHROUGH_INTENDED;
            case 99:
              CRCtriplet(crc, next, -99);
              FALLTHROUGH_INTENDED;
            case 98:
              CRCtriplet(crc, next, -98);
              FALLTHROUGH_INTENDED;
            case 97:
              CRCtriplet(crc, next, -97);
              FALLTHROUGH_INTENDED;
            case 96:
              CRCtriplet(crc, next, -96);
              FALLTHROUGH_INTENDED;
            case 95:
              CRCtriplet(crc, next, -95);
              FALLTHROUGH_INTENDED;
            case 94:
              CRCtriplet(crc, next, -94);
              FALLTHROUGH_INTENDED;
            case 93:
              CRCtriplet(crc, next, -93);
              FALLTHROUGH_INTENDED;
            case 92:
              CRCtriplet(crc, next, -92);
              FALLTHROUGH_INTENDED;
            case 91:
              CRCtriplet(crc, next, -91);
              FALLTHROUGH_INTENDED;
            case 90:
              CRCtriplet(crc, next, -90);
              FALLTHROUGH_INTENDED;
            case 89:
              CRCtriplet(crc, next, -89);
              FALLTHROUGH_INTENDED;
            case 88:
              CRCtriplet(crc, next, -88);
              FALLTHROUGH_INTENDED;
            case 87:
              CRCtriplet(crc, next, -87);
              FALLTHROUGH_INTENDED;
            case 86:
              CRCtriplet(crc, next, -86);
              FALLTHROUGH_INTENDED;
            case 85:
              CRCtriplet(crc, next, -85);
              FALLTHROUGH_INTENDED;
            case 84:
              CRCtriplet(crc, next, -84);
              FALLTHROUGH_INTENDED;
            case 83:
              CRCtriplet(crc, next, -83);
              FALLTHROUGH_INTENDED;
            case 82:
              CRCtriplet(crc, next, -82);
              FALLTHROUGH_INTENDED;
            case 81:
              CRCtriplet(crc, next, -81);
              FALLTHROUGH_INTENDED;
            case 80:
              CRCtriplet(crc, next, -80);
              FALLTHROUGH_INTENDED;
            case 79:
              CRCtriplet(crc, next, -79);
              FALLTHROUGH_INTENDED;
            case 78:
              CRCtriplet(crc, next, -78);
              FALLTHROUGH_INTENDED;
            case 77:
              CRCtriplet(crc, next, -77);
              FALLTHROUGH_INTENDED;
            case 76:
              CRCtriplet(crc, next, -76);
              FALLTHROUGH_INTENDED;
            case 75:
              CRCtriplet(crc, next, -75);
              FALLTHROUGH_INTENDED;
            case 74:
              CRCtriplet(crc, next, -74);
              FALLTHROUGH_INTENDED;
            case 73:
              CRCtriplet(crc, next, -73);
              FALLTHROUGH_INTENDED;
            case 72:
              CRCtriplet(crc, next, -72);
              FALLTHROUGH_INTENDED;
            case 71:
              CRCtriplet(crc, next, -71);
              FALLTHROUGH_INTENDED;
            case 70:
              CRCtriplet(crc, next, -70);
              FALLTHROUGH_INTENDED;
            case 69:
              CRCtriplet(crc, next, -69);
              FALLTHROUGH_INTENDED;
            case 68:
              CRCtriplet(crc, next, -68);
              FALLTHROUGH_INTENDED;
            case 67:
              CRCtriplet(crc, next, -67);
              FALLTHROUGH_INTENDED;
            case 66:
              CRCtriplet(crc, next, -66);
              FALLTHROUGH_INTENDED;
            case 65:
              CRCtriplet(crc, next, -65);
              FALLTHROUGH_INTENDED;
            case 64:
              CRCtriplet(crc, next, -64);
              FALLTHROUGH_INTENDED;
            case 63:
              CRCtriplet(crc, next, -63);
              FALLTHROUGH_INTENDED;
            case 62:
              CRCtriplet(crc, next, -62);
              FALLTHROUGH_INTENDED;
            case 61:
              CRCtriplet(crc, next, -61);
              FALLTHROUGH_INTENDED;
            case 60:
              CRCtriplet(crc, next, -60);
              FALLTHROUGH_INTENDED;
            case 59:
              CRCtriplet(crc, next, -59);
              FALLTHROUGH_INTENDED;
            case 58:
              CRCtriplet(crc, next, -58);
              FALLTHROUGH_INTENDED;
            case 57:
              CRCtriplet(crc, next, -57);
              FALLTHROUGH_INTENDED;
            case 56:
              CRCtriplet(crc, next, -56);
              FALLTHROUGH_INTENDED;
            case 55:
              CRCtriplet(crc, next, -55);
              FALLTHROUGH_INTENDED;
            case 54:
              CRCtriplet(crc, next, -54);
              FALLTHROUGH_INTENDED;
            case 53:
              CRCtriplet(crc, next, -53);
              FALLTHROUGH_INTENDED;
            case 52:
              CRCtriplet(crc, next, -52);
              FALLTHROUGH_INTENDED;
            case 51:
              CRCtriplet(crc, next, -51);
              FALLTHROUGH_INTENDED;
            case 50:
              CRCtriplet(crc, next, -50);
              FALLTHROUGH_INTENDED;
            case 49:
              CRCtriplet(crc, next, -49);
              FALLTHROUGH_INTENDED;
            case 48:
              CRCtriplet(crc, next, -48);
              FALLTHROUGH_INTENDED;
            case 47:
              CRCtriplet(crc, next, -47);
              FALLTHROUGH_INTENDED;
            case 46:
              CRCtriplet(crc, next, -46);
              FALLTHROUGH_INTENDED;
            case 45:
              CRCtriplet(crc, next, -45);
              FALLTHROUGH_INTENDED;
            case 44:
              CRCtriplet(crc, next, -44);
              FALLTHROUGH_INTENDED;
            case 43:
              CRCtriplet(crc, next, -43);
              FALLTHROUGH_INTENDED;
            case 42:
              CRCtriplet(crc, next, -42);
              FALLTHROUGH_INTENDED;
            case 41:
              CRCtriplet(crc, next, -41);
              FALLTHROUGH_INTENDED;
            case 40:
              CRCtriplet(crc, next, -40);
              FALLTHROUGH_INTENDED;
            case 39:
              CRCtriplet(crc, next, -39);
              FALLTHROUGH_INTENDED;
            case 38:
              CRCtriplet(crc, next, -38);
              FALLTHROUGH_INTENDED;
            case 37:
              CRCtriplet(crc, next, -37);
              FALLTHROUGH_INTENDED;
            case 36:
              CRCtriplet(crc, next, -36);
              FALLTHROUGH_INTENDED;
            case 35:
              CRCtriplet(crc, next, -35);
              FALLTHROUGH_INTENDED;
            case 34:
              CRCtriplet(crc, next, -34);
              FALLTHROUGH_INTENDED;
            case 33:
              CRCtriplet(crc, next, -33);
              FALLTHROUGH_INTENDED;
            case 32:
              CRCtriplet(crc, next, -32);
              FALLTHROUGH_INTENDED;
            case 31:
              CRCtriplet(crc, next, -31);
              FALLTHROUGH_INTENDED;
            case 30:
              CRCtriplet(crc, next, -30);
              FALLTHROUGH_INTENDED;
            case 29:
              CRCtriplet(crc, next, -29);
              FALLTHROUGH_INTENDED;
            case 28:
              CRCtriplet(crc, next, -28);
              FALLTHROUGH_INTENDED;
            case 27:
              CRCtriplet(crc, next, -27);
              FALLTHROUGH_INTENDED;
            case 26:
              CRCtriplet(crc, next, -26);
              FALLTHROUGH_INTENDED;
            case 25:
              CRCtriplet(crc, next, -25);
              FALLTHROUGH_INTENDED;
            case 24:
              CRCtriplet(crc, next, -24);
              FALLTHROUGH_INTENDED;
            case 23:
              CRCtriplet(crc, next, -23);
              FALLTHROUGH_INTENDED;
            case 22:
              CRCtriplet(crc, next, -22);
              FALLTHROUGH_INTENDED;
            case 21:
              CRCtriplet(crc, next, -21);
              FALLTHROUGH_INTENDED;
            case 20:
              CRCtriplet(crc, next, -20);
              FALLTHROUGH_INTENDED;
            case 19:
              CRCtriplet(crc, next, -19);
              FALLTHROUGH_INTENDED;
            case 18:
              CRCtriplet(crc, next, -18);
              FALLTHROUGH_INTENDED;
            case 17:
              CRCtriplet(crc, next, -17);
              FALLTHROUGH_INTENDED;
            case 16:
              CRCtriplet(crc, next, -16);
              FALLTHROUGH_INTENDED;
            case 15:
              CRCtriplet(crc, next, -15);
              FALLTHROUGH_INTENDED;
            case 14:
              CRCtriplet(crc, next, -14);
              FALLTHROUGH_INTENDED;
            case 13:
              CRCtriplet(crc, next, -13);
              FALLTHROUGH_INTENDED;
            case 12:
              CRCtriplet(crc, next, -12);
              FALLTHROUGH_INTENDED;
            case 11:
              CRCtriplet(crc, next, -11);
              FALLTHROUGH_INTENDED;
            case 10:
              CRCtriplet(crc, next, -10);
              FALLTHROUGH_INTENDED;
            case 9:
              CRCtriplet(crc, next, -9);
              FALLTHROUGH_INTENDED;
            case 8:
              CRCtriplet(crc, next, -8);
              FALLTHROUGH_INTENDED;
            case 7:
              CRCtriplet(crc, next, -7);
              FALLTHROUGH_INTENDED;
            case 6:
              CRCtriplet(crc, next, -6);
              FALLTHROUGH_INTENDED;
            case 5:
              CRCtriplet(crc, next, -5);
              FALLTHROUGH_INTENDED;
            case 4:
              CRCtriplet(crc, next, -4);
              FALLTHROUGH_INTENDED;
            case 3:
              CRCtriplet(crc, next, -3);
              FALLTHROUGH_INTENDED;
            case 2:
              CRCtriplet(crc, next, -2);
              FALLTHROUGH_INTENDED;
            case 1:
              CRCduplet(crc, next, -1); // the final triplet is actually only 2
              //{ CombineCRC(); }
              crc0 = CombineCRC(block_size, crc0, crc1, crc2, next2);
              if (--n > 0) {
                crc1 = crc2 = 0;
                block_size = 128;
                // points to the first byte of the next block
                next0 = next2 + 128;
                next1 = next0 + 128; // from here on all blocks are 128 long
                next2 = next1 + 128;
              }
              FALLTHROUGH_INTENDED;
            case 0:;
          } while (n > 0);
      }
      next = (const unsigned char*)next2;
    }
    uint64_t count2 = len >> 3; // 216 of less bytes is 27 or less singlets
    len = len & 7;
    next += (count2 * 8);
    switch (count2) {
      case 27:
        CRCsinglet(crc0, next, -27 * 8);
        FALLTHROUGH_INTENDED;
      case 26:
        CRCsinglet(crc0, next, -26 * 8);
        FALLTHROUGH_INTENDED;
      case 25:
        CRCsinglet(crc0, next, -25 * 8);
        FALLTHROUGH_INTENDED;
      case 24:
        CRCsinglet(crc0, next, -24 * 8);
        FALLTHROUGH_INTENDED;
      case 23:
        CRCsinglet(crc0, next, -23 * 8);
        FALLTHROUGH_INTENDED;
      case 22:
        CRCsinglet(crc0, next, -22 * 8);
        FALLTHROUGH_INTENDED;
      case 21:
        CRCsinglet(crc0, next, -21 * 8);
        FALLTHROUGH_INTENDED;
      case 20:
        CRCsinglet(crc0, next, -20 * 8);
        FALLTHROUGH_INTENDED;
      case 19:
        CRCsinglet(crc0, next, -19 * 8);
        FALLTHROUGH_INTENDED;
      case 18:
        CRCsinglet(crc0, next, -18 * 8);
        FALLTHROUGH_INTENDED;
      case 17:
        CRCsinglet(crc0, next, -17 * 8);
        FALLTHROUGH_INTENDED;
      case 16:
        CRCsinglet(crc0, next, -16 * 8);
        FALLTHROUGH_INTENDED;
      case 15:
        CRCsinglet(crc0, next, -15 * 8);
        FALLTHROUGH_INTENDED;
      case 14:
        CRCsinglet(crc0, next, -14 * 8);
        FALLTHROUGH_INTENDED;
      case 13:
        CRCsinglet(crc0, next, -13 * 8);
        FALLTHROUGH_INTENDED;
      case 12:
        CRCsinglet(crc0, next, -12 * 8);
        FALLTHROUGH_INTENDED;
      case 11:
        CRCsinglet(crc0, next, -11 * 8);
        FALLTHROUGH_INTENDED;
      case 10:
        CRCsinglet(crc0, next, -10 * 8);
        FALLTHROUGH_INTENDED;
      case 9:
        CRCsinglet(crc0, next, -9 * 8);
        FALLTHROUGH_INTENDED;
      case 8:
        CRCsinglet(crc0, next, -8 * 8);
        FALLTHROUGH_INTENDED;
      case 7:
        CRCsinglet(crc0, next, -7 * 8);
        FALLTHROUGH_INTENDED;
      case 6:
        CRCsinglet(crc0, next, -6 * 8);
        FALLTHROUGH_INTENDED;
      case 5:
        CRCsinglet(crc0, next, -5 * 8);
        FALLTHROUGH_INTENDED;
      case 4:
        CRCsinglet(crc0, next, -4 * 8);
        FALLTHROUGH_INTENDED;
      case 3:
        CRCsinglet(crc0, next, -3 * 8);
        FALLTHROUGH_INTENDED;
      case 2:
        CRCsinglet(crc0, next, -2 * 8);
        FALLTHROUGH_INTENDED;
      case 1:
        CRCsinglet(crc0, next, -1 * 8);
        FALLTHROUGH_INTENDED;
      case 0:;
    }
  }
  {
    align_to_8(len, crc0, next);
    return (uint32_t)crc0 ^ 0xffffffffu;
  }
}

#endif //HAVE_SSE42 && HAVE_PCLMUL

static inline Function Choose_Extend() {
#ifndef HAVE_POWER8
  if (isSSE42()) {
    if (isPCLMULQDQ()) {
#if defined HAVE_SSE42  && defined HAVE_PCLMUL && !defined NO_THREEWAY_CRC32C
      return crc32c_3way;
#else
    return ExtendImpl<Fast_CRC32>; // Fast_CRC32 will check HAVE_SSE42 itself
#endif
    }
    else {  // no runtime PCLMULQDQ support but has SSE42 support
      return ExtendImpl<Fast_CRC32>;
    }
  } // end of isSSE42()
  else {
    return ExtendImpl<Slow_CRC32>;
  }
#else  //HAVE_POWER8
  return isAltiVec() ? ExtendPPCImpl : ExtendImpl<Slow_CRC32>;
#endif
}

static Function ChosenExtend = Choose_Extend();
uint32_t Extend(uint32_t crc, const char* buf, size_t size) {
  return ChosenExtend(crc, buf, size);
}


}  // namespace crc32c
}  // namespace rocksdb
