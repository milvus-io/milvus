// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "mol_c.h"

#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// RDKit includes
#include <GraphMol/GraphMol.h>
#include <GraphMol/SmilesParse/SmilesParse.h>
#include <GraphMol/SmilesParse/SmilesWrite.h>
#include <GraphMol/MolPickler.h>
#include <GraphMol/Fingerprints/Fingerprints.h>
#include <GraphMol/Fingerprints/MorganFingerprints.h>
#include <GraphMol/Fingerprints/MACCS.h>
#include <DataStructs/ExplicitBitVect.h>

namespace {

MolDataResult CreateErrorResult(int32_t error_code, const char* error_msg) {
    MolDataResult result{};
    result.error_code = error_code;
    result.error_msg = error_msg ? strdup(error_msg) : nullptr;
    return result;
}

MolDataResult CreateSuccessResult(const uint8_t* data, size_t size) {
    MolDataResult result{};
    result.data = static_cast<uint8_t*>(malloc(size));
    if (!result.data) {
        return CreateErrorResult(MOL_ERROR_MEMORY, "Failed to allocate memory");
    }
    memcpy(result.data, data, size);
    result.size = size;
    return result;
}

std::vector<uint8_t> BitVectToBinaryVector(const ExplicitBitVect& bv) {
    size_t num_bits = bv.getNumBits();
    size_t num_bytes = (num_bits + 7) / 8;
    std::vector<uint8_t> result(num_bytes, 0);
    for (size_t i = 0; i < num_bits; ++i) {
        if (bv.getBit(i)) {
            result[i / 8] |= (1 << (i % 8));
        }
    }
    return result;
}

MolDataResult GenerateFingerprintImpl(
    const char* smiles,
    const std::function<ExplicitBitVect*(RDKit::ROMol&)>& gen_fp) {
    if (!smiles || !*smiles) {
        return CreateErrorResult(MOL_ERROR_INVALID_SMILES, "Empty SMILES string");
    }
    try {
        std::unique_ptr<RDKit::ROMol> mol(RDKit::SmilesToMol(smiles));
        if (!mol) {
            return CreateErrorResult(MOL_ERROR_INVALID_SMILES, "Failed to parse SMILES string");
        }
        std::unique_ptr<ExplicitBitVect> fp(gen_fp(*mol));
        if (!fp) {
            return CreateErrorResult(MOL_ERROR_FINGERPRINT_FAILED, "Failed to generate fingerprint");
        }
        auto binary = BitVectToBinaryVector(*fp);
        return CreateSuccessResult(binary.data(), binary.size());
    } catch (const std::exception& e) {
        return CreateErrorResult(MOL_ERROR_FINGERPRINT_FAILED, e.what());
    }
}

}  // namespace

extern "C" {

void FreeMolDataResult(MolDataResult* result) {
    if (result) {
        if (result->data) {
            free(result->data);
            result->data = nullptr;
        }
        if (result->error_msg) {
            free(result->error_msg);
            result->error_msg = nullptr;
        }
        result->size = 0;
        result->error_code = 0;
    }
}

MolDataResult ConvertSMILESToPickle(const char* smiles) {
    if (!smiles || !*smiles) {
        return CreateErrorResult(MOL_ERROR_INVALID_SMILES, "Empty SMILES string");
    }
    try {
        std::unique_ptr<RDKit::ROMol> mol(RDKit::SmilesToMol(smiles));
        if (!mol) {
            return CreateErrorResult(MOL_ERROR_INVALID_SMILES, "Failed to parse SMILES string");
        }
        std::string pickle;
        RDKit::MolPickler::pickleMol(*mol, pickle);
        if (pickle.empty()) {
            return CreateErrorResult(MOL_ERROR_PICKLE_FAILED, "Failed to pickle molecule");
        }
        return CreateSuccessResult(reinterpret_cast<const uint8_t*>(pickle.data()), pickle.size());
    } catch (const std::exception& e) {
        return CreateErrorResult(MOL_ERROR_INVALID_SMILES, e.what());
    }
}

MolDataResult ConvertPickleToSMILES(const uint8_t* pickle_data, size_t pickle_size) {
    if (!pickle_data || pickle_size == 0) {
        return CreateErrorResult(MOL_ERROR_PICKLE_FAILED, "Empty pickle data");
    }
    try {
        std::string pickle_str(reinterpret_cast<const char*>(pickle_data), pickle_size);
        RDKit::ROMol mol;
        RDKit::MolPickler::molFromPickle(pickle_str, &mol);
        std::string smiles = RDKit::MolToSmiles(mol);
        if (smiles.empty()) {
            return CreateErrorResult(MOL_ERROR_PICKLE_FAILED, "Failed to convert molecule to SMILES");
        }
        return CreateSuccessResult(reinterpret_cast<const uint8_t*>(smiles.c_str()), smiles.size() + 1);
    } catch (const std::exception& e) {
        return CreateErrorResult(MOL_ERROR_PICKLE_FAILED, e.what());
    }
}

MolDataResult GenerateMorganFingerprint(const char* smiles, int radius, int fingerprint_size) {
    if (radius < 0 || fingerprint_size <= 0) {
        return CreateErrorResult(MOL_ERROR_FINGERPRINT_FAILED, "Invalid fingerprint parameters");
    }
    return GenerateFingerprintImpl(smiles, [=](RDKit::ROMol& mol) {
        return RDKit::MorganFingerprints::getFingerprintAsBitVect(mol, radius, fingerprint_size);
    });
}

MolDataResult GenerateMACCSFingerprint(const char* smiles) {
    return GenerateFingerprintImpl(smiles, [](RDKit::ROMol& mol) {
        return RDKit::MACCSFingerprints::getFingerprintAsBitVect(mol);
    });
}

MolDataResult GenerateRDKitFingerprint(const char* smiles, int min_path, int max_path, int fingerprint_size) {
    if (min_path < 1 || max_path < min_path || fingerprint_size <= 0) {
        return CreateErrorResult(MOL_ERROR_FINGERPRINT_FAILED, "Invalid fingerprint parameters");
    }
    return GenerateFingerprintImpl(smiles, [=](RDKit::ROMol& mol) {
        return RDKit::RDKFingerprintMol(mol, min_path, max_path, fingerprint_size);
    });
}

}  // extern "C"
