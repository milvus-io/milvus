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

// This is linked into the UnitTests test executable

#include <boost/test/unit_test.hpp>

#include "thrift/concurrency/Exception.h"
#include "thrift/concurrency/Mutex.h"

using boost::unit_test::test_suite;
using boost::unit_test::framework::master_test_suite;

using namespace apache::thrift::concurrency;

struct LFAT
{
  LFAT()
    : uut(Mutex::ERRORCHECK_INITIALIZER)
  {
    BOOST_CHECK_EQUAL(0, pthread_mutex_init(&mx, 0));
    BOOST_CHECK_EQUAL(0, pthread_cond_init(&cv, 0));
  }

  Mutex uut;
  pthread_mutex_t mx;
  pthread_cond_t cv;
};

// Helper for testing mutex behavior when locked by another thread
void * lockFromAnotherThread(void *ptr)
{
  struct LFAT *lfat = (LFAT *)ptr;
  BOOST_CHECK_EQUAL   (0, pthread_mutex_lock(&lfat->mx));           // synchronize with testing thread
  BOOST_CHECK_NO_THROW( lfat->uut.lock());
  BOOST_CHECK_EQUAL   (0, pthread_cond_signal(&lfat->cv));          // tell testing thread we have locked the mutex
  BOOST_CHECK_EQUAL   (0, pthread_cond_wait(&lfat->cv, &lfat->mx)); // wait for testing thread to signal condition variable telling us to unlock
  BOOST_CHECK_NO_THROW( lfat->uut.unlock());
  return ptr;                                                       // testing thread should join to ensure completeness
}

BOOST_AUTO_TEST_SUITE(MutexTest)

BOOST_AUTO_TEST_CASE(happy_path)
{
  Mutex uut(Mutex::ERRORCHECK_INITIALIZER);                         // needed to test unlocking twice without undefined behavior

  BOOST_CHECK_NO_THROW( uut.lock());
  BOOST_CHECK_THROW   ( uut.lock(), SystemResourceException);       // EDEADLK (this thread owns it)
  BOOST_CHECK_NO_THROW( uut.unlock());
}

BOOST_AUTO_TEST_CASE(recursive_happy_path)
{
  Mutex uut(Mutex::RECURSIVE_INITIALIZER);

  BOOST_CHECK_NO_THROW( uut.lock());
  BOOST_CHECK_NO_THROW( uut.lock());
  BOOST_CHECK_NO_THROW( uut.unlock());
  BOOST_CHECK_NO_THROW( uut.lock());
  BOOST_CHECK_NO_THROW( uut.lock());
  BOOST_CHECK_NO_THROW( uut.unlock());
  BOOST_CHECK_NO_THROW( uut.lock());
  BOOST_CHECK_NO_THROW( uut.unlock());
  BOOST_CHECK_NO_THROW( uut.unlock());
  BOOST_CHECK_NO_THROW( uut.unlock());
}

BOOST_AUTO_TEST_CASE(trylock)
{
  Mutex uut(Mutex::ADAPTIVE_INITIALIZER);   // just using another initializer for coverage

  BOOST_CHECK         ( uut.trylock());
  BOOST_CHECK         (!uut.trylock());
  BOOST_CHECK_NO_THROW( uut.unlock());
}

BOOST_AUTO_TEST_CASE(timedlock)
{
  pthread_t th;
  struct LFAT lfat;

  BOOST_CHECK         ( lfat.uut.timedlock(100));
  BOOST_CHECK_THROW   ( lfat.uut.timedlock(100),
                        SystemResourceException);                   // EDEADLK (current thread owns mutex - logic error)
  BOOST_CHECK_NO_THROW( lfat.uut.unlock());

  BOOST_CHECK_EQUAL   (0, pthread_mutex_lock(&lfat.mx));            // synchronize with helper thread
  BOOST_CHECK_EQUAL   (0, pthread_create(&th, NULL,
                            lockFromAnotherThread, &lfat));         // create helper thread
  BOOST_CHECK_EQUAL   (0, pthread_cond_wait(&lfat.cv, &lfat.mx));   // wait for helper thread to lock mutex

  BOOST_CHECK         (!lfat.uut.timedlock(100));                   // false: another thread owns the lock

  BOOST_CHECK_EQUAL   (0, pthread_cond_signal(&lfat.cv));           // tell helper thread we are done
  BOOST_CHECK_EQUAL   (0, pthread_mutex_unlock(&lfat.mx));          // let helper thread clean up
  BOOST_CHECK_EQUAL   (0, pthread_join(th, 0));                     // wait for testing thread to unlock and be done
}

BOOST_AUTO_TEST_CASE(underlying)
{
  Mutex uut;

  BOOST_CHECK         ( uut.getUnderlyingImpl());
}

BOOST_AUTO_TEST_SUITE_END()
