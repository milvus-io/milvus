// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifdef DefineVectorValueType

DefineVectorValueType(Int8, std::int8_t)
DefineVectorValueType(UInt8, std::uint8_t)
DefineVectorValueType(Int16, std::int16_t)
DefineVectorValueType(Float, float)

#endif // DefineVectorValueType


#ifdef DefineDistCalcMethod

DefineDistCalcMethod(L2)
DefineDistCalcMethod(Cosine)

#endif // DefineDistCalcMethod


#ifdef DefineErrorCode

// 0x0000 ~ 0x0FFF  General Status
DefineErrorCode(Success, 0x0000)
DefineErrorCode(Fail, 0x0001)
DefineErrorCode(FailedOpenFile, 0x0002)
DefineErrorCode(FailedCreateFile, 0x0003)
DefineErrorCode(ParamNotFound, 0x0010)
DefineErrorCode(FailedParseValue, 0x0011)
DefineErrorCode(MemoryOverFlow, 0x0012)
DefineErrorCode(LackOfInputs, 0x0013)

// 0x1000 ~ 0x1FFF  Index Build Status

// 0x2000 ~ 0x2FFF  Index Serve Status

// 0x3000 ~ 0x3FFF  Helper Function Status
DefineErrorCode(ReadIni_FailedParseSection, 0x3000)
DefineErrorCode(ReadIni_FailedParseParam, 0x3001)
DefineErrorCode(ReadIni_DuplicatedSection, 0x3002)
DefineErrorCode(ReadIni_DuplicatedParam, 0x3003)


// 0x4000 ~ 0x4FFF Socket Library Status
DefineErrorCode(Socket_FailedResolveEndPoint, 0x4000)
DefineErrorCode(Socket_FailedConnectToEndPoint, 0x4001)


#endif // DefineErrorCode



#ifdef DefineIndexAlgo

DefineIndexAlgo(BKT)
DefineIndexAlgo(KDT)

#endif // DefineIndexAlgo
