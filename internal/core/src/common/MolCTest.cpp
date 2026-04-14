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

#include <gtest/gtest.h>

#include "common/mol_c.h"

TEST(MolCTest, ConvertSMILESToPickleUsesStrictParsing) {
    auto result = ConvertSMILESToPickle("FP(F)(F)(F)(F)F");
    EXPECT_EQ(result.error_code, MOL_ERROR_INVALID_SMILES);
    EXPECT_EQ(result.data, nullptr);
    FreeMolDataResult(&result);
}

TEST(MolCTest, ParseSMILESToMolUsesStrictParsing) {
    auto handle = ParseSMILESToMol("F[Si](F)(F)(F)(F)F");
    EXPECT_EQ(handle, nullptr);
}

TEST(MolCTest, StrictParserStillAcceptsValidSmiles) {
    auto result = ConvertSMILESToPickle("CCO");
    EXPECT_EQ(result.error_code, MOL_SUCCESS);
    EXPECT_NE(result.data, nullptr);
    EXPECT_GT(result.size, 0);
    FreeMolDataResult(&result);
}

TEST(MolCTest, ConvertPickleToSmilesRejectsEmptyInput) {
    auto result = ConvertPickleToSMILES(nullptr, 0);
    EXPECT_EQ(result.error_code, MOL_ERROR_PICKLE_FAILED);
    EXPECT_EQ(result.data, nullptr);
    FreeMolDataResult(&result);
}

TEST(MolCTest, SmilesPickleSmilesRoundTripSucceeds) {
    auto pickle = ConvertSMILESToPickle("CCO");
    ASSERT_EQ(pickle.error_code, MOL_SUCCESS);
    ASSERT_NE(pickle.data, nullptr);
    ASSERT_GT(pickle.size, 0U);

    auto smiles = ConvertPickleToSMILES(pickle.data, pickle.size);
    EXPECT_EQ(smiles.error_code, MOL_SUCCESS);
    EXPECT_NE(smiles.data, nullptr);
    EXPECT_GT(smiles.size, 0U);

    FreeMolDataResult(&smiles);
    FreeMolDataResult(&pickle);
}

TEST(MolCTest, SubstructMatchRejectsEmptyInputs) {
    EXPECT_LT(HasSubstructMatch(nullptr, 0, nullptr, 0), 0);

    auto query_handle = ParseSMILESToMol("CO");
    EXPECT_LT(HasSubstructMatchWithQuery(nullptr, 0, query_handle), 0);
    FreeMolHandle(query_handle);

    auto mol_handle = ParseSMILESToMol("CCO");
    EXPECT_LT(HasSubstructMatchWithMol(mol_handle, nullptr, 0), 0);
    FreeMolHandle(mol_handle);

    EXPECT_LT(HasSubstructMatchHandles(nullptr, nullptr), 0);
}

TEST(MolCTest, SubstructMatchFindsExpectedHits) {
    auto mol = ConvertSMILESToPickle("CCO");
    auto query = ConvertSMILESToPickle("CO");
    auto no_match = ConvertSMILESToPickle("c1ccccc1");

    ASSERT_EQ(mol.error_code, MOL_SUCCESS);
    ASSERT_EQ(query.error_code, MOL_SUCCESS);
    ASSERT_EQ(no_match.error_code, MOL_SUCCESS);

    EXPECT_EQ(HasSubstructMatch(mol.data, mol.size, query.data, query.size), 1);
    EXPECT_EQ(HasSubstructMatch(mol.data, mol.size, no_match.data, no_match.size), 0);

    FreeMolDataResult(&no_match);
    FreeMolDataResult(&query);
    FreeMolDataResult(&mol);
}

TEST(MolCTest, SubstructMatchWithHandlesWorks) {
    auto mol_pickle = ConvertSMILESToPickle("CCO");
    auto query_pickle = ConvertSMILESToPickle("CO");
    ASSERT_EQ(mol_pickle.error_code, MOL_SUCCESS);
    ASSERT_EQ(query_pickle.error_code, MOL_SUCCESS);

    auto query_handle = ParseSMILESToMol("CO");
    EXPECT_EQ(HasSubstructMatchWithQuery(mol_pickle.data, mol_pickle.size, query_handle), 1);

    auto mol_handle = ParseSMILESToMol("CCO");
    EXPECT_EQ(HasSubstructMatchWithMol(mol_handle, query_pickle.data, query_pickle.size), 1);

    auto no_match_handle = ParseSMILESToMol("c1ccccc1");
    EXPECT_EQ(HasSubstructMatchHandles(mol_handle, no_match_handle), 0);

    FreeMolHandle(no_match_handle);
    FreeMolHandle(mol_handle);
    FreeMolHandle(query_handle);
    FreeMolDataResult(&query_pickle);
    FreeMolDataResult(&mol_pickle);
}
