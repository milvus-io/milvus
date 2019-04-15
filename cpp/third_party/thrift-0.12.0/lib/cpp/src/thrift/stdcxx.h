/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _THRIFT_STDCXX_H_
#define _THRIFT_STDCXX_H_ 1

#include <boost/config.hpp>
#include <boost/version.hpp>

///////////////////////////////////////////////////////////////////
//
// functional (function, bind)
//
///////////////////////////////////////////////////////////////////

#if defined(BOOST_NO_CXX11_HDR_FUNCTIONAL) || (defined(_MSC_VER) && _MSC_VER < 1800) || defined(FORCE_BOOST_FUNCTIONAL)
#if (BOOST_VERSION <= 106500)
#include <boost/tr1/functional.hpp>
#else
#include <tr1/functional>
#endif
#define _THRIFT_FUNCTIONAL_TR1_ 1
#endif

#if _THRIFT_FUNCTIONAL_TR1_

  namespace apache { namespace thrift { namespace stdcxx {

    using ::std::tr1::bind;
    using ::std::tr1::function;

    namespace placeholders {
      using ::std::tr1::placeholders::_1;
      using ::std::tr1::placeholders::_2;
      using ::std::tr1::placeholders::_3;
      using ::std::tr1::placeholders::_4;
      using ::std::tr1::placeholders::_5;
      using ::std::tr1::placeholders::_6;
      using ::std::tr1::placeholders::_7;
      using ::std::tr1::placeholders::_8;
      using ::std::tr1::placeholders::_9;
    } // apache::thrift::stdcxx::placeholders
  }}} // apache::thrift::stdcxx

#else

  #include <functional>

  namespace apache { namespace thrift { namespace stdcxx {
    using ::std::bind;
    using ::std::function;

    namespace placeholders {
      using ::std::placeholders::_1;
      using ::std::placeholders::_2;
      using ::std::placeholders::_3;
      using ::std::placeholders::_4;
      using ::std::placeholders::_5;
      using ::std::placeholders::_6;
      using ::std::placeholders::_7;
      using ::std::placeholders::_8;
      using ::std::placeholders::_9;
    } // apache::thrift::stdcxx::placeholders
  }}} // apache::thrift::stdcxx

#endif

///////////////////////////////////////////////////////////////////
//
// Smart Pointers
//
///////////////////////////////////////////////////////////////////

// We can use std for memory functions only if the compiler supports template aliasing
// The macro BOOST_NO_CXX11_SMART_PTR is defined as 1 under Visual Studio 2010 and 2012
// which do not support the feature, so we must continue to use C++98 and boost on them.
// We cannot use __cplusplus to detect this either, since Microsoft advertises an older one.

#if defined(BOOST_NO_CXX11_SMART_PTR) || (defined(_MSC_VER) && _MSC_VER < 1800) || defined(FORCE_BOOST_SMART_PTR)
#include <boost/smart_ptr.hpp>
#else
#include <memory>
#endif

namespace apache { namespace thrift { namespace stdcxx {

#if defined(BOOST_NO_CXX11_SMART_PTR) || (defined(_MSC_VER) && _MSC_VER < 1800) || defined(FORCE_BOOST_SMART_PTR)

  using ::boost::const_pointer_cast;
  using ::boost::dynamic_pointer_cast;
  using ::boost::enable_shared_from_this;
  using ::boost::make_shared;
  using ::boost::scoped_ptr;
  using ::boost::shared_ptr;
  using ::boost::static_pointer_cast;
  using ::boost::weak_ptr;

#else

  using ::std::const_pointer_cast;
  using ::std::dynamic_pointer_cast;
  using ::std::enable_shared_from_this;
  using ::std::make_shared;
  template <typename T> using scoped_ptr = std::unique_ptr<T>;		// compiler must support template aliasing
  using ::std::shared_ptr;
  using ::std::static_pointer_cast;
  using ::std::weak_ptr;

#endif

}}} // apache::thrift::stdcxx

#endif // #ifndef _THRIFT_STDCXX_H_
