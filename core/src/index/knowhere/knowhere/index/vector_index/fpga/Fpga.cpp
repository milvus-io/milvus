// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>

#include <armadillo>
#include "knowhere/index/vector_index/fpga/Fpga.h"
#include "knowhere/index/vector_index/fpga/utils.h"
#include "knowhere/index/vector_index/fpga/xilinx_c.h"

namespace Fpga {
FpgaInterface::~FpgaInterface() {
}

void
FpgaInterface::GetDevAddr() {
    ci_s = 0;
    tm2_s = ci_s + nlist + 10;
    db_s = tm2_s + (nb / 64 + 10 + nlist);
    pq_s = db_s + (nb / (256 / m) + 4 * nlist);
    que_s = pq_s + 256 + 10;
}

void
FpgaInterface::GetConfigTable(idx_t nprobe) {
    step1_s = que_s + 1020;
    if ((nq * nlist * 32) % 2048 == 0) {
        step2_s = step1_s + ((nq * nlist * 32) / 2048);
    } else {
        step2_s = step1_s + ((nq * 8 * m) / 2048);
    }

    step3_s = step2_s + (nq * 256 / (64 / m));
    final_s = step2_s;

    cfg_table = {nprobe, nq, nlist, m, ci_s, tm2_s, db_s, pq_s, que_s, step1_s, step2_s, step3_s};

    /*if (verbose) {
        auto iter = cfg_table.begin();
        while (iter != cfg_table.end()) {
            LOG_KNOWHERE_DEBUG_ << *iter;
            iter++;
        }
    }*/
}

void
FpgaInterface::TransFile2FPGA(const char* filename, loff_t base, size_t offset) {
    uint32_t file_size;
    loff_t dst_addr = base + offset;

    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (fin.is_open()) {
        fin.seekg(0, std::ios::end);
        file_size = fin.tellg();
        fin.close();
    }

    DMAFile2FPGA((char*)DEVICE_H2C0, dst_addr, file_size, 0, 1, filename);
}

char*
FpgaInterface::LoadFile2Mem(const char* filename, size_t& file_size, char* buffer) {
    // size_t file_size;
    loff_t base_addr;

    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (fin.is_open()) {
        fin.seekg(0, std::ios::end);
        file_size = fin.tellg();
        fin.close();
    }

    // posix_memalign((void **)&buffer, 4096/*alignment*/, file_size + 4096);
    // assert(buffer);

    return ReadFile2Mem(filename, buffer, file_size);
}

void
FpgaInterface::TransMem2FPGA(char* buffer, size_t size, loff_t offset) {
    char* buffer0 = NULL;
    char* buffer1 = NULL;
    char* buffer2 = NULL;
    char* buffer3 = NULL;

    posix_memalign((void**)&buffer0, 4096 /*alignment*/, size / 4 + 4096);
    assert(buffer0);
    posix_memalign((void**)&buffer1, 4096 /*alignment*/, size / 4 + 4096);
    assert(buffer1);
    posix_memalign((void**)&buffer2, 4096 /*alignment*/, size / 4 + 4096);
    assert(buffer2);
    posix_memalign((void**)&buffer3, 4096 /*alignment*/, size / 4 + 4096);
    assert(buffer3);

    SplitMemory(buffer, size, buffer0, buffer1, buffer2, buffer3);
    TransBuffer2FPGA(buffer0, size / 4, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_0 + offset, 1);
    TransBuffer2FPGA(buffer1, size / 4, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_1 + offset, 1);
    TransBuffer2FPGA(buffer2, size / 4, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_2 + offset, 1);
    TransBuffer2FPGA(buffer3, size / 4, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_3 + offset, 1);

    free(buffer0);
    free(buffer1);
    free(buffer2);
    free(buffer3);
}

void
FpgaInterface::WriteFileFromFPGA(const char* filename, size_t size, loff_t base, size_t offset) {
    loff_t src_addr = base + offset;

    DMAFPGA2File((char*)DEVICE_C2H0, src_addr, size, 0, 1, filename);
}

char*
FpgaInterface::LoadFPGA2Mem(loff_t fpga_addr, char* buffer, size_t size) {
    // char *buffer = NULL;

    // posix_memalign((void **)&buffer, 4096/*alignment*/, size + 4096);
    // assert(buffer);

    // memset(buffer, 0x00, size);

    return TransFPGA2Mem((char*)DEVICE_C2H0, fpga_addr, size, buffer, 1);
}

void
FpgaInterface::SaveMem2File(char* buffer, size_t size, const char* filename) {
    WriteMem2File(buffer, size, 1, filename);

    // free(buffer);
}

int
FpgaInterface::OpenCtrlDev() {
    return open_dev((char*)DEVICE_CTRL);
}

int
FpgaInterface::CloseCtrlDev(int fd) {
    return close_dev(fd);
}

int
FpgaInterface::WritePara(int fd, uint32_t addr, uint32_t val) {
    return reg_rw(fd, PARA_BASE_ADDR + addr, (char*)"write", 'w', val);
}

int
FpgaInterface::ReadPara(int fd, uint32_t addr) {
    return reg_rw(fd, PARA_BASE_ADDR + addr, (char*)"read", 'w', 0);
}

int
FpgaInterface::WriteReg(int fd, uint32_t addr, uint32_t val) {
    return reg_rw(fd, REG_BASE_ADDR + addr, (char*)"write", 'w', val);
}

int
FpgaInterface::ReadReg(int fd, uint32_t addr) {
    return reg_rw(fd, REG_BASE_ADDR + addr, (char*)"read", 'w', 0);
}

void
FpgaInterface::TriggerSearch(int fd) {
    WritePara(fd, TRIGGER_REG_ADDR, 0);
    WriteReg(fd, 0, (0x1 << 6));
}

uint32_t
FpgaInterface::GetSearchTime(int fd) {
    int index;
    std::vector<uint32_t> vec(8, 0);
    uint32_t search_time = 0;

    for (index = 0; index < 8; index++) {
        vec[index] = ReadPara(fd, Time_REG_ADDR + index * 4) & 0xFFFFFFFF;
    }

    search_time = (vec[6] - vec[0]) * (1000 / FPGA_RATE) / 1000000;

    return search_time;
}

template <typename T>
static arma::Cube<T>
CubeTranspose(arma::Cube<T>& cube) {
    arma::uword D1 = cube.n_rows;
    arma::uword D2 = cube.n_cols;
    arma::uword D3 = cube.n_slices;
    arma::Cube<T> output(D1, D3, D2);

    for (arma::uword s = 0; s < D3; ++s) {
        for (arma::uword c = 0; c < D2; ++c) {
            for (arma::uword r = 0; r < D1; ++r) {
                output.at(r, s, c) = cube.at(r, c, s);
                // output[ D1*D3*c + D1*s+ r ] = cube[ D1*D2*s + D1*c + r ];
            }
        }
    }
    return output;
}

static void
cal_term2(faiss::IndexIVFPQ* index, std::vector<double>* dis_term2, std::vector<double>* all_codes,
          std::vector<int>* all_list_size, std::vector<idx_t>* all_ids) {
    int nb = index->ntotal;

    int term2_count = 0;

    for (size_t i = 0; i < index->nlist; i++) {
        all_list_size->at(i) = (index->invlists->list_size(i));
        const uint8_t* codes = index->invlists->get_codes(i);
        const idx_t* ids = index->invlists->get_ids(i);
        for (size_t j = 0; j < index->invlists->list_size(i); j++) {
            all_ids->at(term2_count) = ids[j];

            dis_term2->at(term2_count) = 0;
            for (size_t m = 0; m < index->pq.M; m++) {
                all_codes->at(term2_count * index->pq.M + m) = codes[m];
                dis_term2->at(term2_count) +=
                    index->precomputed_table[i * index->pq.M * index->pq.ksub + m * index->pq.ksub + codes[m]];
            }
            codes += index->pq.code_size;
            term2_count += 1;
        }
    }
    assert(term2_count == nb);
}

static void
cal_ci(faiss::IndexIVFPQ* index, std::vector<float>& ci_vec) {
    index->quantizer->reconstruct_n(0, index->nlist, ci_vec.data());

    for (size_t i = 0; i < ci_vec.size(); i++) {
        ci_vec[i] = -ci_vec[i];
    }
}

void
FpgaInterface::CopyIndexToFPGA() {
    {
        // Step1: Transfer coarse_centroids
        std::vector<float> ci(nlist * d, 0);
        cal_ci(index_ptr, ci);

        TransMem2FPGA((char*)ci.data(), ci.size() * sizeof(float), ci_s * 64);

        // WriteMem2File((char *)ci_negitve.data(), ci_negitve.size()*sizeof(float), 1, "coarse_centroids.bin");
    }

    {
        // Step2: Transfer pq_quantizer
        float* centroids_ptr = &index_ptr->pq.centroids[0];
        arma::fcube PQ(centroids_ptr, index_ptr->pq.dsub, index_ptr->pq.ksub, index_ptr->pq.M);
        PQ = CubeTranspose(PQ);
        TransMem2FPGA((char*)PQ.memptr(), PQ.size() * sizeof(float), pq_s * 64);

        // WriteMem2File((char *)PQ.memptr(), PQ.size()*sizeof(float), 1, "pq_quantizer.bin");
    }

    {
        // Step3: all_list_size and all_index_id
        std::vector<double> dis_term2(nb, 0);
        std::vector<double> all_codes(nb * m, 0);
        cal_term2(index_ptr, &dis_term2, &all_codes, &all_list_size, &all_index_id);

        // code database
        int database_width = m * 8;
        int alignment_remain = 8192 % database_width;
        assert(alignment_remain <= 0);
        int alignment_num = (int)(8192 / database_width);
        int index_base = 0;
        std::vector<double> database;
        for (size_t i = 0; i < all_list_size.size(); i++) {
            int current_size = all_list_size[i];
            auto iter = all_codes.begin();
            database.insert(database.end(), iter + index_base * m, iter + (index_base + current_size) * m);

            int page_size = ceil((float)current_size / alignment_num) * alignment_num;

            for (size_t j = 0; j < (page_size - current_size) * m; j++) {
                database.push_back(3.14);
            }

            index_base += current_size;
        }
        std::vector<uint8_t> database_u8(database.size());
        for (size_t i = 0; i < database.size(); i++) {
            database_u8[i] = (uint8_t)database[i];
        }
        TransMem2FPGA((char*)database_u8.data(), database_u8.size(), db_s * 64);

        // term2
        index_base = 0;
        std::vector<double> term2_temp;
        for (size_t i = 0; i < all_list_size.size(); i++) {
            int current_size = all_list_size[i];
            auto iter = dis_term2.begin();
            term2_temp.insert(term2_temp.end(), iter + index_base, iter + index_base + current_size);

            int ceil_size = ceil((float)current_size / 64) * 64;
            for (size_t j = 0; j < ceil_size - current_size; j++) {
                term2_temp.push_back(3.14);
            }

            index_base += current_size;
        }
        std::vector<float> term2_flt(term2_temp.size());
        for (size_t i = 0; i < term2_temp.size(); i++) {
            term2_flt[i] = (float)term2_temp[i];
        }
        TransMem2FPGA((char*)term2_flt.data(), term2_flt.size() * sizeof(float), tm2_s * 64);
    }
}

int
FpgaInterface::GetDatabaseListAlign(std::vector<int>& data_list, uint32_t align, std::vector<int>& align_list) {
    auto iter = data_list.begin();

    while (iter != data_list.end()) {
        int temp = (int)(ceil((float)*iter / align) * align);  // round up the value
        align_list.push_back(temp);
        iter++;
    }

    return 0;
}

void
FpgaInterface::GetDatabaseListSum(std::vector<int>& data_vec, std::vector<int>& sum_vec) {
    int temp = 0;
    auto iter = data_vec.begin();

    while (iter != data_vec.end()) {
        sum_vec.push_back(temp);
        temp += *iter;
        iter++;
    }
}

void
FpgaInterface::GetProbeDatabaseAddressList(uint32_t base_addr, std::vector<int>& data_vec, int align,
                                           std::vector<uint32_t>& addr_vec, uint32_t m) {
    std::vector<int> offset_vec;
    std::vector<int> sum_vec;

    GetDatabaseListAlign(data_vec, align, offset_vec);
    GetDatabaseListSum(offset_vec, sum_vec);

    auto iter = sum_vec.begin();
    while (iter != sum_vec.end()) {
        uint32_t addr = base_addr + ((*iter) * m / 256);
        addr_vec.push_back(addr);
        iter++;
    }
}

void
FpgaInterface::GetProbeTerm2AddressList(uint32_t base_addr, std::vector<int>& data_vec, int align,
                                        std::vector<uint32_t>& addr_vec) {
    std::vector<int> offset_vec;
    std::vector<int> sum_vec;
    uint32_t addr;

    GetDatabaseListAlign(data_vec, align, offset_vec);
    GetDatabaseListSum(offset_vec, sum_vec);

    auto iter = sum_vec.begin();
    while (iter != sum_vec.end()) {
        addr = base_addr + ((*iter) * 32 / 2048);
        addr_vec.push_back(addr);
        iter++;
    }
}

int
FpgaInterface::CfgDataBaseNumReal(int fd) {
    int index = 0;
    std::vector<uint32_t> database_vec;
    std::vector<uint32_t> term2_vec;
    uint32_t addr = cfg_table.size() * 4;

    uint32_t align = 8192 / m / 8;
    GetProbeDatabaseAddressList((uint32_t)db_s, all_list_size, align, database_vec, m);

    GetProbeTerm2AddressList(tm2_s, all_list_size, 64, term2_vec);

    for (index = 0; index < nlist; index++) {
        WritePara(fd, addr, all_list_size[index]);
        addr += 4;
    }

    for (index = 0; index < nlist; index++) {
        WritePara(fd, addr, database_vec[index]);
        addr += 4;
    }

    for (index = 0; index < nlist; index++) {
        WritePara(fd, addr, term2_vec[index]);
        addr += 4;
    }

    return 0;
}

void
FpgaInterface::TransferCfg() {
    int index = 0;
    int fd;

    fd = OpenCtrlDev();

    auto iter = cfg_table.begin();
    while (iter != cfg_table.end()) {
        WritePara(fd, index, *iter);
        iter++;
        index += 4;
    }

    CfgDataBaseNumReal(fd);

    CloseCtrlDev(fd);
}

void
FpgaInterface::FlushResultMem() {
    uint32_t buffer_size = topk * nq * 4 * 2 / 4;
    uint32_t offset = step3_s * 64;
    char* flush_buffer = new char[buffer_size];

    memset(flush_buffer, 0x1, buffer_size);

    TransBuffer2FPGA(flush_buffer, buffer_size, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_0 + offset, 1);
    TransBuffer2FPGA(flush_buffer, buffer_size, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_1 + offset, 1);
    TransBuffer2FPGA(flush_buffer, buffer_size, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_2 + offset, 1);
    TransBuffer2FPGA(flush_buffer, buffer_size, (char*)DEVICE_H2C0, FPGA_BASE_ADDR_3 + offset, 1);

    delete[] flush_buffer;
}

void
FpgaInterface::StartFPGASearch() {
    int index = 0;
    int fd = -1;

    fd = OpenCtrlDev();
    TriggerSearch(fd);

    while (index < 10000) {
        if (ReadPara(fd, TRIGGER_REG_ADDR)) {
            break;
        }
        index++;
        sleep(0.001);
    }
    // milvus::LOG_ENGINE_DEBUG_ << " Trigger FPGA search";

    if (verbose) {
        GetSearchTime(fd);
    }

    CloseCtrlDev(fd);
}

void
ReadSingleBlock(char* buffer, std::vector<uint32_t>& label_vec, std::vector<float>& dis_vec) {
    int index;
    uint32_t* label = NULL;
    float* distance = NULL;

    for (index = 0; index < 8; index++) {
        label = (uint32_t*)(buffer + index * 8);
        distance = reinterpret_cast<float*>(buffer + index * 8 + 4);
        label_vec.push_back(*label);
        dis_vec.push_back(*distance);
    }
}

void
FpgaInterface::ReadResultFile(std::vector<idx_t>& id_vec, std::vector<float>& dis_vec) {
    uint32_t index;
    uint32_t offset = step3_s * 64;
    uint32_t single_size = topk * nq * 4 * 2 / 4;

    std::vector<uint32_t> id_vec_0;
    std::vector<uint32_t> id_vec_1;
    std::vector<float> dis_vec_0;
    std::vector<float> dis_vec_1;

    char* buffer0 = NULL;
    char* buffer1 = NULL;
    char* buffer2 = NULL;
    char* buffer3 = NULL;

    posix_memalign((void**)&buffer0, 4096 /*alignment*/, single_size + 4096);
    assert(buffer0);
    posix_memalign((void**)&buffer1, 4096 /*alignment*/, single_size + 4096);
    assert(buffer1);
    posix_memalign((void**)&buffer2, 4096 /*alignment*/, single_size + 4096);
    assert(buffer2);
    posix_memalign((void**)&buffer3, 4096 /*alignment*/, single_size + 4096);
    assert(buffer3);

    LoadFPGA2Mem(FPGA_BASE_ADDR_0 + offset, buffer0, single_size);
    LoadFPGA2Mem(FPGA_BASE_ADDR_1 + offset, buffer1, single_size);
    LoadFPGA2Mem(FPGA_BASE_ADDR_2 + offset, buffer2, single_size);
    LoadFPGA2Mem(FPGA_BASE_ADDR_3 + offset, buffer3, single_size);

    /* extract labels and distances from FPGA result file */
    uint32_t block_num = single_size / 64;
    for (index = 0; index < block_num; index++) {
        if (index < block_num / 2) {
            ReadSingleBlock(buffer0 + index * 64, id_vec_0, dis_vec_0);
            ReadSingleBlock(buffer1 + index * 64, id_vec_0, dis_vec_0);
            ReadSingleBlock(buffer2 + index * 64, id_vec_0, dis_vec_0);
            ReadSingleBlock(buffer3 + index * 64, id_vec_0, dis_vec_0);
        } else {
            ReadSingleBlock(buffer0 + index * 64, id_vec_1, dis_vec_1);
            ReadSingleBlock(buffer1 + index * 64, id_vec_1, dis_vec_1);
            ReadSingleBlock(buffer2 + index * 64, id_vec_1, dis_vec_1);
            ReadSingleBlock(buffer3 + index * 64, id_vec_1, dis_vec_1);
        }
    }

    /* assemble labels and distances */
    for (index = 0; index < nq; index++) {
        for (int j = 0; j < 64; j++) {
            // cout << index << " " << j << " " << id_vec_0[index*64+j] << " " << id_vec_0.size()<<endl;
            id_vec[index * 128 + j] = all_index_id[id_vec_0[index * 64 + j]];
            dis_vec[index * 128 + j] = dis_vec_0[index * 64 + j];
        }
        for (int z = 0; z < 64; z++) {
            id_vec[index * 128 + z + 64] = all_index_id[id_vec_1[index * 64 + z]];
            dis_vec[index * 128 + z + 64] = dis_vec_1[index * 64 + z];
        }
    }
}

bool
FpgaInterface::BatchAnnQuery(idx_t nprobe, idx_t batch_size, float* items, idx_t expect_k, std::vector<idx_t>& id_vec,
                             std::vector<float>& dis_vec) {
    topk = expect_k;

    if (first_query || nq != batch_size || pre_nprobe != nprobe) {
        pre_nprobe = nprobe;
        nq = batch_size;
        GetConfigTable(nprobe);
        TransferCfg();
        if (first_query || last_field < nq * topk) {
            FlushResultMem();
            first_query = false;
            last_field = nq * topk;
        }
    }
    size_t query_size = batch_size * d * sizeof(float);
    TransMem2FPGA((char*)items, query_size, que_s * 64);
    StartFPGASearch();
    ReadResultFile(id_vec, dis_vec);
}
}  // namespace Fpga
