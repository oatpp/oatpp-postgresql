/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present, Leonid Stryzhevskyi <lganzzzo@gmail.com>
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

#include "ArrayTest.hpp"

#include "oatpp-postgresql/orm.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"

namespace oatpp { namespace test { namespace postgresql { namespace types {

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

class Row1 : public oatpp::DTO {

  DTO_INIT(Row1, DTO);

  DTO_FIELD(Vector<Float32>, f_real);
  DTO_FIELD(Vector<Float64>, f_double);
  DTO_FIELD(Vector<Int16>, f_int16);
  DTO_FIELD(Vector<Int32>, f_int32);
  DTO_FIELD(Vector<Int64>, f_int64);
  DTO_FIELD(Vector<Boolean>, f_bool);
  DTO_FIELD(Vector<String>, f_text);

};

#include OATPP_CODEGEN_END(DTO)

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::orm::DbClient {
public:

  MyClient(const std::shared_ptr<oatpp::orm::Executor>& executor)
    : oatpp::orm::DbClient(executor)
  {

    executeQuery("DROP TABLE IF EXISTS oatpp_schema_version_ArrayTest;", {});

    oatpp::orm::SchemaMigration migration(executor, "ArrayTest");
    migration.addFile(1, TEST_DB_MIGRATION "ArrayTest.sql");
    migration.migrate();

    auto version = executor->getSchemaVersion("ArrayTest");
    OATPP_LOGD("DbClient", "Migration - OK. Version=%d.", version);

  }

  QUERY(insertValues1,
        "INSERT INTO test_arrays1 "
        "(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text) "
        "VALUES "
        "(:row.f_real, :row.f_double, :row.f_int16, :row.f_int32, :row.f_int64, :row.f_bool, :row.f_text);",
        PARAM(oatpp::Object<Row1>, row), PREPARE(true))

  QUERY(selectValues1, "SELECT * FROM test_arrays1;")

};

#include OATPP_CODEGEN_END(DbClient)

}

void ArrayTest::onRun() {

  OATPP_LOGI(TAG, "DB-URL='%s'", TEST_DB_URL);

  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionProvider);

  auto client = MyClient(executor);

  {
    auto row = Row1::createShared();
    row->f_real = {nullptr, v_float32(0), v_float32(1)};
    row->f_double = {nullptr, v_float64(0), v_float64(1)};
    row->f_int16 = {nullptr, v_int16(0), v_int16(16)};
    row->f_int32 = {nullptr, v_int32(0), v_int32(32)};
    row->f_int64 = {nullptr, v_int64(0), v_int64(64)};
    row->f_bool = {nullptr, false, true};
    row->f_text = {nullptr, "", "Hello"};

    auto res = client.insertValues1(row);
    if(res->isSuccess()) {
      OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
    }

  }

  {
    auto res = client.selectValues1();
    if(res->isSuccess()) {
      OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
    }

    auto dataset = res->fetch<oatpp::Vector<oatpp::Object<Row1>>>();

    OATPP_ASSERT(dataset->size() == 6)

    {
      auto row = dataset[0];

      OATPP_ASSERT(row->f_real == nullptr);
      OATPP_ASSERT(row->f_double == nullptr);
      OATPP_ASSERT(row->f_int16 == nullptr);
      OATPP_ASSERT(row->f_int32 == nullptr);
      OATPP_ASSERT(row->f_int64 == nullptr);
      OATPP_ASSERT(row->f_bool == nullptr);
      OATPP_ASSERT(row->f_text == nullptr);

    }

    {
      auto row = dataset[1];

      OATPP_ASSERT(row->f_real != nullptr);
      OATPP_ASSERT(row->f_double != nullptr);
      OATPP_ASSERT(row->f_int16 != nullptr);
      OATPP_ASSERT(row->f_int32 != nullptr);
      OATPP_ASSERT(row->f_int64 != nullptr);
      OATPP_ASSERT(row->f_bool != nullptr);
      OATPP_ASSERT(row->f_text != nullptr);

      OATPP_ASSERT(row->f_real->size() == 0);
      OATPP_ASSERT(row->f_double->size() == 0);
      OATPP_ASSERT(row->f_int16->size() == 0);
      OATPP_ASSERT(row->f_int32->size() == 0);
      OATPP_ASSERT(row->f_int64->size() == 0);
      OATPP_ASSERT(row->f_bool->size() == 0);
      OATPP_ASSERT(row->f_text->size() == 0);

    }

    {
      auto row = dataset[2];

      OATPP_ASSERT(row->f_real != nullptr);
      OATPP_ASSERT(row->f_double != nullptr);
      OATPP_ASSERT(row->f_int16 != nullptr);
      OATPP_ASSERT(row->f_int32 != nullptr);
      OATPP_ASSERT(row->f_int64 != nullptr);
      OATPP_ASSERT(row->f_bool != nullptr);
      OATPP_ASSERT(row->f_text != nullptr);

      OATPP_ASSERT(row->f_real->size() == 2);
      OATPP_ASSERT(row->f_double->size() == 2);
      OATPP_ASSERT(row->f_int16->size() == 2);
      OATPP_ASSERT(row->f_int32->size() == 2);
      OATPP_ASSERT(row->f_int64->size() == 2);
      OATPP_ASSERT(row->f_bool->size() == 2);
      OATPP_ASSERT(row->f_text->size() == 2);

      OATPP_ASSERT(row->f_real[0] == nullptr && row->f_real[1] == nullptr);
      OATPP_ASSERT(row->f_double[0] == nullptr && row->f_double[1] == nullptr);
      OATPP_ASSERT(row->f_int16[0] == nullptr && row->f_int16[1] == nullptr);
      OATPP_ASSERT(row->f_int32[0] == nullptr && row->f_int32[1] == nullptr);
      OATPP_ASSERT(row->f_int64[0] == nullptr && row->f_int64[1] == nullptr);
      OATPP_ASSERT(row->f_bool[0] == nullptr && row->f_bool[1] == nullptr);
      OATPP_ASSERT(row->f_text[0] == nullptr && row->f_text[1] == nullptr);

    }

    {
      auto row = dataset[3];

      OATPP_ASSERT(row->f_real != nullptr);
      OATPP_ASSERT(row->f_double != nullptr);
      OATPP_ASSERT(row->f_int16 != nullptr);
      OATPP_ASSERT(row->f_int32 != nullptr);
      OATPP_ASSERT(row->f_int64 != nullptr);
      OATPP_ASSERT(row->f_bool != nullptr);
      OATPP_ASSERT(row->f_text != nullptr);

      OATPP_ASSERT(row->f_real->size() == 1);
      OATPP_ASSERT(row->f_double->size() == 1);
      OATPP_ASSERT(row->f_int16->size() == 1);
      OATPP_ASSERT(row->f_int32->size() == 1);
      OATPP_ASSERT(row->f_int64->size() == 1);
      OATPP_ASSERT(row->f_bool->size() == 1);
      OATPP_ASSERT(row->f_text->size() == 2);

      OATPP_ASSERT(row->f_real[0] == v_float32(0));
      OATPP_ASSERT(row->f_double[0] == v_float64(0));
      OATPP_ASSERT(row->f_int16[0] == v_int16(0));
      OATPP_ASSERT(row->f_int32[0] == v_int32(0));
      OATPP_ASSERT(row->f_int64[0] == v_int64(0));
      OATPP_ASSERT(row->f_bool[0] == false);
      OATPP_ASSERT(row->f_text[0] == "" && row->f_text[1] == "");

    }

    {
      auto row = dataset[4];

      OATPP_ASSERT(row->f_real != nullptr);
      OATPP_ASSERT(row->f_double != nullptr);
      OATPP_ASSERT(row->f_int16 != nullptr);
      OATPP_ASSERT(row->f_int32 != nullptr);
      OATPP_ASSERT(row->f_int64 != nullptr);
      OATPP_ASSERT(row->f_bool != nullptr);
      OATPP_ASSERT(row->f_text != nullptr);

      OATPP_ASSERT(row->f_real->size() == 1);
      OATPP_ASSERT(row->f_double->size() == 1);
      OATPP_ASSERT(row->f_int16->size() == 1);
      OATPP_ASSERT(row->f_int32->size() == 1);
      OATPP_ASSERT(row->f_int64->size() == 1);
      OATPP_ASSERT(row->f_bool->size() == 1);
      OATPP_ASSERT(row->f_text->size() == 1);

      OATPP_ASSERT(row->f_real[0] == v_float32(1));
      OATPP_ASSERT(row->f_double[0] == v_float64(1));
      OATPP_ASSERT(row->f_int16[0] == v_int16(1));
      OATPP_ASSERT(row->f_int32[0] == v_int32(1));
      OATPP_ASSERT(row->f_int64[0] == v_int64(1));
      OATPP_ASSERT(row->f_bool[0] == true);
      OATPP_ASSERT(row->f_text[0] == "hello");

    }

    {
      auto row = dataset[5];

      OATPP_ASSERT(row->f_real != nullptr);
      OATPP_ASSERT(row->f_double != nullptr);
      OATPP_ASSERT(row->f_int16 != nullptr);
      OATPP_ASSERT(row->f_int32 != nullptr);
      OATPP_ASSERT(row->f_int64 != nullptr);
      OATPP_ASSERT(row->f_bool != nullptr);
      OATPP_ASSERT(row->f_text != nullptr);

      OATPP_ASSERT(row->f_real->size() == 3);
      OATPP_ASSERT(row->f_double->size() == 3);
      OATPP_ASSERT(row->f_int16->size() == 3);
      OATPP_ASSERT(row->f_int32->size() == 3);
      OATPP_ASSERT(row->f_int64->size() == 3);
      OATPP_ASSERT(row->f_bool->size() == 3);
      OATPP_ASSERT(row->f_text->size() == 3);

      OATPP_ASSERT(row->f_real[0] == nullptr && row->f_real[1] == v_float32(0) && row->f_real[2] == v_float32(1));
      OATPP_ASSERT(row->f_double[0] == nullptr && row->f_double[1] == v_float64(0) && row->f_double[2] == v_float64(1));
      OATPP_ASSERT(row->f_int16[0] == nullptr && row->f_int16[1] == v_int16(0) && row->f_int16[2] == v_int16(16));
      OATPP_ASSERT(row->f_int32[0] == nullptr && row->f_int32[1] == v_int32(0) && row->f_int32[2] == v_int32(32));
      OATPP_ASSERT(row->f_int64[0] == nullptr && row->f_int64[1] == v_int64(0) && row->f_int64[2] == v_int64(64));
      OATPP_ASSERT(row->f_bool[0] == nullptr && row->f_bool[1] == false && row->f_bool[2] == true);
      OATPP_ASSERT(row->f_text[0] == nullptr && row->f_text[1] == "" && row->f_text[2] == "Hello");

    }

  }


}

}}}}
