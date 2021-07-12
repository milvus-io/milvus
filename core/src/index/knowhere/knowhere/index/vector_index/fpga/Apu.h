//
// Created by ezeharia on 5/12/21.
//

#ifdef MILVUS_APU_VERSION
#ifndef APU_H
#define APU_H


#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>

extern "C"
{
#include <gsi/libgsl.h> // added by eitan
#include <gsi/libgsl_matrix.h>
#include <gsi/libgsl_flat_tanimoto.h>
#include <gsi/libgdl.h>
}

#define NUM_QUERIES (100)
#define CHAR_BITS (8)
#define INIT_K (50)

namespace Fpga {

enum class APU_CLEAN_TYPE {

    SESSION_HDL = 0,
    BDBH = 1 ,
    FULL,
    NEW_DB ,
};

enum class APU_METRIC_TYPE {
    HAMMING =0 ,
    TANIMOTO =1 ,
};

class ApuInterface {

 public :

    ApuInterface();

    ~ApuInterface();

    void PopulateApuParams (uint32_t dimention , uint32_t row_count , const std::string location);

    void createBdb();

    void loadSeesionToApu ( APU_METRIC_TYPE type);

    void Query(gsl_matrix_u32& indices ,gsl_matrix_f32& distances ,gsl_matrix_u1& queries , APU_METRIC_TYPE type);

    bool isLoadNeeded(std::string location_);

    void cleanApuResources(APU_CLEAN_TYPE type);

private :

    gsl_bdb_hdl bdbh_ =NULL ;

    void InitApu();

    void loadHammingSessionToApu ();

    void loadTanimotoSessionToApu ();

    std::string location_ ="";

    uint32_t num_records_ = 0;

    uint32_t num_bfeatures_ = 0;

    uint32_t num_bytes_in_rec_ = 0;

    gsl_context gsl_ctx_ ;

    struct gsl_matrix_u1 bdb_ ;

    gsl_search_session_hdl session_hdl_;

    gsl_flat_hamming_desc hamming_desc_;

    gsl_flat_tanimoto_desc tanimoto_desc_;

    uint32_t topK_ = INIT_K;
public:
    uint32_t getTopK() const;

    void setTopK(uint32_t topK);




};//ApuInterface
using ApuInterfacePtr = std::shared_ptr<ApuInterface>;
}//Fpga

#endif //MILVUS_ENGINE_APU_H
#endif
