/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************************/

#ifndef oatpp_test_core_data_mapping_type_ByteArrayTypeTest_hpp
#define oatpp_test_core_data_mapping_type_ByteArrayTypeTest_hpp

#include "oatpp-test/UnitTest.hpp"

namespace oatpp {
namespace test {
namespace postgresql {
namespace types {

class ByteArrayTypeTest : public UnitTest {
  public:
    ByteArrayTypeTest() : UnitTest("TEST[oatpp::test::postgresql::types::ByteArrayTypeTest]") {}
    void onRun() override;
};

} // namespace types
} // namespace postgresql
} // namespace test
} // namespace oatpp

#endif /* oatpp_test_core_data_mapping_type_ByteArrayTypeTest_hpp */
