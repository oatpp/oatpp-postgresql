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

#include <limits>
#include <cstdio>
#include <iostream>

namespace oatpp { namespace test { namespace postgresql { namespace types {

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

class Row : public oatpp::DTO {

  DTO_INIT(Row, DTO);

  DTO_FIELD(Vector<Float32>, f_real);
  DTO_FIELD(Vector<Float64>, f_double);
  DTO_FIELD(Vector<Int16>, f_int16);
  DTO_FIELD(Vector<Int32>, f_int32);
  DTO_FIELD(Vector<Int64>, f_int64);
  DTO_FIELD(Vector<Boolean> , f_bool);
  DTO_FIELD(Vector<String> , f_text);

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

  QUERY(insertValues,
        "INSERT INTO test_arrays1 "
        "(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text) "
        "VALUES "
        "(:row.f_real, :row.f_double, :row.f_int16, :row.f_int32, :row.f_int64, :row.f_bool, :row.f_text);",
        PARAM(oatpp::Object<Row>, row), PREPARE(true))

  QUERY(deleteValues,
        "DELETE FROM test_floats;")

  QUERY(selectValues, "SELECT * FROM test_arrays1;")

};

#include OATPP_CODEGEN_END(DbClient)

}

void ArrayTest::onRun() {

  OATPP_LOGI(TAG, "DB-URL='%s'", TEST_DB_URL);

  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionProvider);

  auto client = MyClient(executor);

  {
    auto row = Row::createShared();
    row->f_real = {nullptr, v_float32(0), 0.32};
    row->f_double = {nullptr, v_float64 (0), 0.64};
    row->f_int16 = {nullptr, v_int16(0), 16};
    row->f_int32 = {nullptr, v_int16(0), 32};
    row->f_int64 = {nullptr, v_int16(0), 64};
    row->f_bool = {nullptr, true, false};
    row->f_text = {nullptr, "", "Hello", "World!"};

    auto res = client.insertValues(row);
    if(res->isSuccess()) {
      OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
    }

  }

  {
    auto res = client.selectValues();
    if(res->isSuccess()) {
      OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
    }

    auto dataset = res->fetch<oatpp::Vector<oatpp::Fields<oatpp::Any>>>();

    oatpp::parser::json::mapping::ObjectMapper om;
    om.getSerializer()->getConfig()->useBeautifier = true;
    om.getSerializer()->getConfig()->enabledInterpretations = {"postgresql"};

    auto str = om.writeToString(dataset);

    std::cout << "\n" << str->std_str() << std::endl;

  }


}

}}}}
