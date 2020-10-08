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

#include "FloatTest.hpp"

#include "oatpp-postgresql/orm.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"

#include <limits>
#include <cstdio>

namespace oatpp { namespace test { namespace postgresql { namespace types {

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

class Row : public oatpp::DTO {

  DTO_INIT(Row, DTO);

  DTO_FIELD(Float32, f_real);
  DTO_FIELD(Float64, f_double);

};

#include OATPP_CODEGEN_END(DTO)

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::orm::DbClient {
public:

  MyClient(const std::shared_ptr<oatpp::orm::Executor>& executor)
    : oatpp::orm::DbClient(executor)
  {

    executeQuery("DROP TABLE IF EXISTS oatpp_schema_version_FloatTest;", {});

    oatpp::orm::SchemaMigration migration(executor, "FloatTest");
    migration.addFile(1, TEST_DB_MIGRATION "FloatTest.sql");
    migration.migrate();

    auto version = executor->getSchemaVersion("FloatTest");
    OATPP_LOGD("DbClient", "Migration - OK. Version=%d.", version);

  }

  QUERY(insertValues,
        "INSERT INTO test_floats "
        "(f_real, f_double) "
        "VALUES "
        "(:row.f_real, :row.f_double);",
        PARAM(oatpp::Object<Row>, row), PREPARE(true))

  QUERY(deleteValues,
        "DELETE FROM test_floats;")

  QUERY(selectValues, "SELECT * FROM test_floats;")

};

#include OATPP_CODEGEN_END(DbClient)

}

void FloatTest::onRun() {

  OATPP_LOGI(TAG, "DB-URL='%s'", TEST_DB_URL);

  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionProvider);

  auto client = MyClient(executor);

  {
    auto res = client.selectValues();
    if(res->isSuccess()) {
      OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
    }

    auto dataset = res->fetch<oatpp::Vector<oatpp::Object<Row>>>();

    oatpp::parser::json::mapping::ObjectMapper om;
    om.getSerializer()->getConfig()->useBeautifier = true;
    om.getSerializer()->getConfig()->enabledInterpretations = {"postgresql"};

    auto str = om.writeToString(dataset);

    OATPP_LOGD(TAG, "res=%s", str->c_str());

    OATPP_ASSERT(dataset->size() == 4);

    {
      auto row = dataset[0];
      OATPP_ASSERT(row->f_real == nullptr);
      OATPP_ASSERT(row->f_double == nullptr);
    }

    {
      auto row = dataset[1];
      OATPP_ASSERT(row->f_real == 0);
      OATPP_ASSERT(row->f_double == 0);
    }

    {
      auto row = dataset[2];
      OATPP_ASSERT(row->f_real == 1);
      OATPP_ASSERT(row->f_double == 2);
    }

    {
      auto row = dataset[3];
      OATPP_ASSERT(row->f_real == -1);
      OATPP_ASSERT(row->f_double == -2);
    }

  }

  {
    auto res = client.deleteValues();
    if (res->isSuccess()) {
      OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
    }

    OATPP_ASSERT(res->isSuccess());
  }

  {
    auto connection = client.getConnection();
    {
      auto row = Row::createShared();
      row->f_real = nullptr;
      row->f_double = nullptr;
      client.insertValues(row, connection);
    }

    {
      auto row = Row::createShared();
      row->f_real = 10;
      row->f_double = 10;
      client.insertValues(row, connection);
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

    auto dataset = res->fetch<oatpp::Vector<oatpp::Object<Row>>>();

    oatpp::parser::json::mapping::ObjectMapper om;
    om.getSerializer()->getConfig()->useBeautifier = true;
    om.getSerializer()->getConfig()->enabledInterpretations = {"postgresql"};

    auto str = om.writeToString(dataset);

    OATPP_LOGD(TAG, "res=%s", str->c_str());

    OATPP_ASSERT(dataset->size() == 2);

    {
      auto row = dataset[0];
      OATPP_ASSERT(row->f_real == nullptr);
      OATPP_ASSERT(row->f_double == nullptr);
    }

    {
      auto row = dataset[1];
      OATPP_ASSERT(row->f_real == 10);
      OATPP_ASSERT(row->f_double == 10);
    }

  }

}

}}}}
