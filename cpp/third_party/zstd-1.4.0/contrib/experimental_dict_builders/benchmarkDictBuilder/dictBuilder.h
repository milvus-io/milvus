/* ZDICT_trainFromBuffer_legacy() :
 * issue : samplesBuffer need to be followed by a noisy guard band.
 * work around : duplicate the buffer, and add the noise */
size_t ZDICT_trainFromBuffer_legacy(void* dictBuffer, size_t dictBufferCapacity,
                                    const void* samplesBuffer, const size_t* samplesSizes, unsigned nbSamples,
                                    ZDICT_legacy_params_t params);
