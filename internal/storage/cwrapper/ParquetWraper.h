#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef void *CPayloadWriter;

typedef struct CBuffer {
  char *data;
  int length;
} CBuffer;

typedef struct CStatus {
  int error_code;
  const char *error_msg;
} CStatus;

CPayloadWriter NewPayloadWriter(int columnType);
CStatus AddBooleanToPayload(CPayloadWriter payloadWriter, bool *values, int length);
CStatus AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t *values, int length);
CStatus AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t *values, int length);
CStatus AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t *values, int length);
CStatus AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t *values, int length);
CStatus AddFloatToPayload(CPayloadWriter payloadWriter, float *values, int length);
CStatus AddDoubleToPayload(CPayloadWriter payloadWriter, double *values, int length);

CStatus AddBinaryVectorToPayload(CPayloadWriter payloadWriter, int8_t *values, int dimension, int length);
CStatus AddFloatVectorToPayload(CPayloadWriter payloadWriter, float *values, int dimension, int length);

CStatus FinishPayload(CPayloadWriter payloadWriter);
CBuffer GetPayloadBuffer(CPayloadWriter payloadWriter);
int GetPayloadNums(CPayloadWriter payloadWriter);
CStatus ReleasePayload(CPayloadWriter handler);

#ifdef __cplusplus
}
#endif