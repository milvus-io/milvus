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


module Thrift.Protocol.Header
    ( module Thrift.Protocol
    , HeaderProtocol(..)
    , getProtocolType
    , setProtocolType
    , getHeaders
    , getWriteHeaders
    , setHeader
    , setHeaders
    , createHeaderProtocol
    , createHeaderProtocol1
    ) where

import Thrift.Protocol
import Thrift.Protocol.Binary
import Thrift.Protocol.JSON
import Thrift.Protocol.Compact
import Thrift.Transport
import Thrift.Transport.Header
import Data.IORef
import qualified Data.Map as Map

data ProtocolWrap = forall a. (Protocol a) => ProtocolWrap(a)

instance Protocol ProtocolWrap where
  readByte (ProtocolWrap p) = readByte p
  readVal (ProtocolWrap p) = readVal p
  readMessage (ProtocolWrap p) = readMessage p
  writeVal (ProtocolWrap p) = writeVal p
  writeMessage (ProtocolWrap p) = writeMessage p

data HeaderProtocol i o = (Transport i, Transport o) => HeaderProtocol {
    trans :: HeaderTransport i o,
    wrappedProto :: IORef ProtocolWrap
  }

createProtocolWrap :: Transport t => ProtocolType -> t -> ProtocolWrap
createProtocolWrap typ t =
  case typ of
    TBinary -> ProtocolWrap $ BinaryProtocol t
    TCompact -> ProtocolWrap $ CompactProtocol t
    TJSON -> ProtocolWrap $ JSONProtocol t

createHeaderProtocol :: (Transport i, Transport o) => i -> o -> IO(HeaderProtocol i o)
createHeaderProtocol i o = do
  t <- openHeaderTransport i o
  pid <- readIORef $ protocolType t
  proto <- newIORef $ createProtocolWrap pid t
  return $ HeaderProtocol { trans = t, wrappedProto = proto }

createHeaderProtocol1 :: Transport t => t -> IO(HeaderProtocol t t)
createHeaderProtocol1 t = createHeaderProtocol t t

resetProtocol :: (Transport i, Transport o) => HeaderProtocol i o -> IO ()
resetProtocol p = do
  pid <- readIORef $ protocolType $ trans p
  writeIORef (wrappedProto p) $ createProtocolWrap pid $ trans p

getWrapped = readIORef . wrappedProto

setTransport :: (Transport i, Transport o) => HeaderProtocol i o -> HeaderTransport i o -> HeaderProtocol i o
setTransport p t = p { trans = t }

updateTransport :: (Transport i, Transport o) => HeaderProtocol i o -> (HeaderTransport i o -> HeaderTransport i o)-> HeaderProtocol i o
updateTransport p f = setTransport p (f $ trans p)

type Headers = Map.Map String String

-- TODO: we want to set headers without recreating client...
setHeader :: (Transport i, Transport o) => HeaderProtocol i o -> String -> String -> HeaderProtocol i o
setHeader p k v = updateTransport p $ \t -> t { writeHeaders = Map.insert k v $ writeHeaders t }

setHeaders :: (Transport i, Transport o) => HeaderProtocol i o -> Headers -> HeaderProtocol i o
setHeaders p h = updateTransport p $ \t -> t { writeHeaders = h }

-- TODO: make it public once we have first transform implementation for Haskell
setTransforms :: (Transport i, Transport o) => HeaderProtocol i o -> [TransformType] -> HeaderProtocol i o
setTransforms p trs = updateTransport p $ \t -> t { writeTransforms = trs }

setTransform :: (Transport i, Transport o) => HeaderProtocol i o -> TransformType -> HeaderProtocol i o
setTransform p tr = updateTransport p $ \t -> t { writeTransforms = tr:(writeTransforms t) }

getWriteHeaders :: (Transport i, Transport o) => HeaderProtocol i o -> Headers
getWriteHeaders = writeHeaders . trans

getHeaders :: (Transport i, Transport o) => HeaderProtocol i o -> IO [(String, String)]
getHeaders = readIORef . headers . trans

getProtocolType :: (Transport i, Transport o) => HeaderProtocol i o -> IO ProtocolType
getProtocolType p = readIORef $ protocolType $ trans p

setProtocolType :: (Transport i, Transport o) => HeaderProtocol i o -> ProtocolType -> IO ()
setProtocolType p typ = do
  typ0 <- getProtocolType p
  if typ == typ0
    then return ()
    else do
      tSetProtocol (trans p) typ
      resetProtocol p

instance (Transport i, Transport o) => Protocol (HeaderProtocol i o) where
  readByte p = tReadAll (trans p) 1

  readVal p tp = do
    proto <- getWrapped p
    readVal proto tp

  readMessage p f = do
    tResetProtocol (trans p)
    resetProtocol p
    proto <- getWrapped p
    readMessage proto f

  writeVal p v = do
    proto <- getWrapped p
    writeVal proto v

  writeMessage p x f = do
    proto <- getWrapped p
    writeMessage proto x f

