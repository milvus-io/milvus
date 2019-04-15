--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

module Thrift.Transport.Header
  ( module Thrift.Transport
  , HeaderTransport(..)
  , openHeaderTransport
  , ProtocolType(..)
  , TransformType(..)
  , ClientType(..)
  , tResetProtocol
  , tSetProtocol
  ) where

import Thrift.Transport
import Thrift.Protocol.Compact
import Control.Applicative
import Control.Exception ( throw )
import Control.Monad
import Data.Bits
import Data.IORef
import Data.Int
import Data.Monoid
import Data.Word

import qualified Data.Attoparsec.ByteString as P
import qualified Data.Binary as Binary
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.Builder as B
import qualified Data.Map as Map

data ProtocolType = TBinary | TCompact | TJSON deriving (Enum, Eq)
data ClientType = HeaderClient | Framed | Unframed deriving (Enum, Eq)

infoIdKeyValue = 1

type Headers = Map.Map String String

data TransformType = ZlibTransform deriving (Enum, Eq)

fromTransportType :: TransformType -> Int16
fromTransportType ZlibTransform = 1

toTransportType :: Int16 -> TransformType
toTransportType 1 = ZlibTransform
toTransportType _ =  throw $ TransportExn "HeaderTransport: Unknown transform ID" TE_UNKNOWN

data HeaderTransport i o = (Transport i, Transport o) => HeaderTransport
    { readBuffer :: IORef LBS.ByteString
    , writeBuffer :: IORef B.Builder
    , inTrans :: i
    , outTrans :: o
    , clientType :: IORef ClientType
    , protocolType :: IORef ProtocolType
    , headers :: IORef [(String, String)]
    , writeHeaders :: Headers
    , transforms :: IORef [TransformType]
    , writeTransforms :: [TransformType]
    }

openHeaderTransport :: (Transport i, Transport o) => i -> o -> IO (HeaderTransport i o)
openHeaderTransport i o = do
  pid <- newIORef TCompact
  rBuf <- newIORef LBS.empty
  wBuf <- newIORef mempty
  cType <- newIORef HeaderClient
  h <- newIORef []
  trans <- newIORef []
  return HeaderTransport
      { readBuffer = rBuf
      , writeBuffer = wBuf
      , inTrans = i
      , outTrans = o
      , clientType = cType
      , protocolType = pid
      , headers = h
      , writeHeaders = Map.empty
      , transforms = trans
      , writeTransforms = []
      }

isFramed t = (/= Unframed) <$> readIORef (clientType t)

readFrame :: (Transport i, Transport o) => HeaderTransport i o -> IO Bool
readFrame t = do
  let input = inTrans t
  let rBuf = readBuffer t
  let cType = clientType t
  lsz <- tRead input 4
  let sz = LBS.toStrict lsz
  case P.parseOnly P.endOfInput sz of
    Right _ -> do return False
    Left _ -> do
      case parseBinaryMagic sz of
        Right _ -> do
          writeIORef rBuf $ lsz
          writeIORef cType Unframed
          writeIORef (protocolType t) TBinary
          return True
        Left _ -> do
          case parseCompactMagic sz of
            Right _ -> do
              writeIORef rBuf $ lsz
              writeIORef cType Unframed
              writeIORef (protocolType t) TCompact
              return True
            Left _ -> do
              let len = Binary.decode lsz :: Int32
              lbuf <- tReadAll input $ fromIntegral len
              let buf = LBS.toStrict lbuf
              case parseBinaryMagic buf of
                Right _ -> do
                  writeIORef cType Framed
                  writeIORef (protocolType t) TBinary
                  writeIORef rBuf lbuf
                  return True
                Left _ -> do
                  case parseCompactMagic buf of
                    Right _ -> do
                      writeIORef cType Framed
                      writeIORef (protocolType t) TCompact
                      writeIORef rBuf lbuf
                      return True
                    Left _ -> do
                      case parseHeaderMagic buf of
                        Right flags -> do
                          let (flags, seqNum, header, body) = extractHeader buf
                          writeIORef cType HeaderClient
                          handleHeader t header
                          payload <- untransform t body
                          writeIORef rBuf $ LBS.fromStrict $ payload
                          return True
                        Left _ ->
                          throw $ TransportExn "HeaderTransport: unkonwn client type" TE_UNKNOWN

parseBinaryMagic = P.parseOnly $ P.word8 0x80 *> P.word8 0x01 *> P.word8 0x00 *> P.anyWord8
parseCompactMagic = P.parseOnly $ P.word8 0x82 *> P.satisfy (\b -> b .&. 0x1f == 0x01)
parseHeaderMagic = P.parseOnly $ P.word8 0x0f *> P.word8 0xff *> (P.count 2 P.anyWord8)

parseI32 :: P.Parser Int32
parseI32 = Binary.decode . LBS.fromStrict <$> P.take 4
parseI16 :: P.Parser Int16
parseI16 = Binary.decode . LBS.fromStrict <$> P.take 2

extractHeader :: BS.ByteString -> (Int16, Int32, BS.ByteString, BS.ByteString)
extractHeader bs =
  case P.parse extractHeader_ bs of
    P.Done remain (flags, seqNum, header) -> (flags, seqNum, header, remain)
    _ -> throw $ TransportExn "HeaderTransport: Invalid header" TE_UNKNOWN
  where
    extractHeader_ = do
      magic <- P.word8 0x0f *> P.word8 0xff
      flags <- parseI16
      seqNum <- parseI32
      (headerSize :: Int) <- (* 4) . fromIntegral <$> parseI16
      header <- P.take headerSize
      return (flags, seqNum, header)

handleHeader t header =
  case P.parseOnly parseHeader header of
    Right (pType, trans, info) -> do
      writeIORef (protocolType t) pType
      writeIORef (transforms t) trans
      writeIORef (headers t) info
    _ -> throw $ TransportExn "HeaderTransport: Invalid header" TE_UNKNOWN


iw16 :: Int16 -> Word16
iw16 = fromIntegral
iw32 :: Int32 -> Word32
iw32 = fromIntegral
wi16 :: Word16 -> Int16
wi16 = fromIntegral
wi32 :: Word32 -> Int32
wi32 = fromIntegral

parseHeader :: P.Parser (ProtocolType, [TransformType], [(String, String)])
parseHeader = do
  protocolType <- toProtocolType <$> parseVarint wi16
  numTrans <- fromIntegral <$> parseVarint wi16
  trans <- replicateM numTrans parseTransform
  info <- parseInfo
  return (protocolType, trans, info)

toProtocolType :: Int16 -> ProtocolType
toProtocolType 0 = TBinary
toProtocolType 1 = TJSON
toProtocolType 2 = TCompact

fromProtocolType :: ProtocolType -> Int16
fromProtocolType TBinary = 0
fromProtocolType TJSON = 1
fromProtocolType TCompact = 2

parseTransform :: P.Parser TransformType
parseTransform = toTransportType <$> parseVarint wi16

parseInfo :: P.Parser [(String, String)]
parseInfo = do
  n <- P.eitherP P.endOfInput (parseVarint wi32)
  case n of
    Left _ -> return []
    Right n0 ->
      replicateM (fromIntegral n0) $ do
        klen <- parseVarint wi16
        k <- P.take $ fromIntegral klen
        vlen <- parseVarint wi16
        v <- P.take $ fromIntegral vlen
        return (C.unpack k, C.unpack v)

parseString :: P.Parser BS.ByteString
parseString = parseVarint wi32 >>= (P.take . fromIntegral)

buildHeader :: HeaderTransport i o -> IO B.Builder
buildHeader t = do
  pType <- readIORef $ protocolType t
  let pId = buildVarint $ iw16 $ fromProtocolType pType
  let headerContent = pId <> (buildTransforms t) <> (buildInfo t)
  let len = fromIntegral $ LBS.length $ B.toLazyByteString headerContent
  -- TODO: length limit check
  let padding = mconcat $ replicate (mod len 4) $ B.word8 0
  let codedLen = B.int16BE (fromIntegral $ (quot (len - 1) 4) + 1)
  let flags = 0
  let seqNum = 0
  return $ B.int16BE 0x0fff <> B.int16BE flags <> B.int32BE seqNum <> codedLen <> headerContent <> padding

buildTransforms :: HeaderTransport i o -> B.Builder
-- TODO: check length limit
buildTransforms t =
  let trans = writeTransforms t in
  (buildVarint $ iw16 $ fromIntegral $ length trans) <>
  (mconcat $ map (buildVarint . iw16 . fromTransportType) trans)

buildInfo :: HeaderTransport i o -> B.Builder
buildInfo t =
  let h = Map.assocs $ writeHeaders t in
  -- TODO: check length limit
  case length h of
    0 -> mempty
    len -> (buildVarint $ iw16 $ fromIntegral $ len) <> (mconcat $ map buildInfoEntry h)
  where
    buildInfoEntry (k, v) = buildVarStr k <> buildVarStr v
    -- TODO: check length limit
    buildVarStr s = (buildVarint $ iw16 $ fromIntegral $ length s) <> B.string8 s

tResetProtocol :: (Transport i, Transport o) => HeaderTransport i o -> IO Bool
tResetProtocol t = do
  rBuf <- readIORef $ readBuffer t
  writeIORef (clientType t) HeaderClient
  readFrame t

tSetProtocol :: (Transport i, Transport o) => HeaderTransport i o -> ProtocolType -> IO ()
tSetProtocol t = writeIORef (protocolType t)

transform :: HeaderTransport i o -> LBS.ByteString -> LBS.ByteString
transform t bs =
  foldr applyTransform bs $ writeTransforms t
  where
    -- applyTransform bs ZlibTransform =
    --   throw $ TransportExn "HeaderTransport: not implemented: ZlibTransform  " TE_UNKNOWN
    applyTransform bs _ =
      throw $ TransportExn "HeaderTransport: Unknown transform" TE_UNKNOWN

untransform :: HeaderTransport i o -> BS.ByteString -> IO BS.ByteString
untransform t bs = do
  trans <- readIORef $ transforms t
  return $ foldl unapplyTransform bs trans
  where
    -- unapplyTransform bs ZlibTransform =
    --   throw $ TransportExn "HeaderTransport: not implemented: ZlibTransform  " TE_UNKNOWN
    unapplyTransform bs _ =
      throw $ TransportExn "HeaderTransport: Unknown transform" TE_UNKNOWN

instance (Transport i, Transport o) => Transport (HeaderTransport i o) where
  tIsOpen t = do
    tIsOpen (inTrans t)
    tIsOpen (outTrans t)

  tClose t = do
    tClose(outTrans t)
    tClose(inTrans t)

  tRead t len = do
    rBuf <- readIORef $ readBuffer t
    if not $ LBS.null rBuf
      then do
        let (consumed, remain) = LBS.splitAt (fromIntegral len) rBuf
        writeIORef (readBuffer t) remain
        return consumed
      else do
        framed <- isFramed t
        if not framed
          then tRead (inTrans t) len
          else do
            ok <- readFrame t
            if ok
              then tRead t len
              else return LBS.empty

  tPeek t = do
    rBuf <- readIORef (readBuffer t)
    if not $ LBS.null rBuf
      then return $ Just $ LBS.head rBuf
      else do
        framed <- isFramed t
        if not framed
          then tPeek (inTrans t)
          else do
            ok <- readFrame t
            if ok
              then tPeek t
              else return Nothing

  tWrite t buf = do
    let wBuf = writeBuffer t
    framed <- isFramed t
    if framed
      then modifyIORef wBuf (<> B.lazyByteString buf)
      else
        -- TODO: what should we do when switched to unframed in the middle ?
        tWrite(outTrans t) buf

  tFlush t = do
    cType <- readIORef $ clientType t
    case cType of
      Unframed -> tFlush $ outTrans t
      Framed -> flushBuffer t id mempty
      HeaderClient -> buildHeader t >>= flushBuffer t (transform t)
    where
      flushBuffer t f header = do
        wBuf <- readIORef $ writeBuffer t
        writeIORef (writeBuffer t) mempty
        let payload = B.toLazyByteString (header <> wBuf)
        tWrite (outTrans t) $ Binary.encode (fromIntegral $ LBS.length payload :: Int32)
        tWrite (outTrans t) $ f payload
        tFlush (outTrans t)
