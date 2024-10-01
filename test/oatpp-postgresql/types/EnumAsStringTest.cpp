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

#include "EnumAsStringTest.hpp"

#include "oatpp-postgresql/orm.hpp"
#include "oatpp/json/ObjectMapper.hpp"

#include <limits>
#include <cstdio>

namespace oatpp { namespace test { namespace postgresql { namespace types {

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

ENUM(Animal, v_int32,
    VALUE(DOG, 0, "dog"),
    VALUE(CAT, 1, "cat"),
    VALUE(BIRD, 2, "bird"),
    VALUE(HORSE, 3, "horse")
)

class Row : public oatpp::DTO {

  DTO_INIT(Row, DTO);

  DTO_FIELD(Enum<Animal>::AsNumber, f_enumint);
  DTO_FIELD(Enum<Animal>::AsString, f_enumstring);

};

#include OATPP_CODEGEN_END(DTO)

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::orm::DbClient {
public:

  MyClient(const std::shared_ptr<oatpp::orm::Executor>& executor)
    : oatpp::orm::DbClient(executor)
  {
    executeQuery("DROP TABLE IF EXISTS oatpp_schema_version_EnumAsStringTest;", {});
    oatpp::orm::SchemaMigration migration(executor, "EnumAsStringTest");
    migration.addFile(1, TEST_DB_MIGRATION "EnumAsStringTest.sql");
    migration.migrate();

    auto version = executor->getSchemaVersion("EnumAsStringTest");
    OATPP_LOGd("DbClient", "Migration - OK. Version={}.", version);

  }

  QUERY(insertValues,
        "INSERT INTO test_EnumAsString "
        "(f_enumint, f_enumstring) "
        "VALUES "
        "(:row.f_enumint, :row.f_enumstring);",
        PARAM(oatpp::Object<Row>, row), PREPARE(true))

  QUERY(deleteValues,
        "DELETE FROM test_EnumAsString;")

  QUERY(selectValues, "SELECT * FROM test_EnumAsString;")

};

#include OATPP_CODEGEN_END(DbClient)

}

void EnumAsStringTest::onRun() {

  OATPP_LOGi(TAG, "DB-URL='{}'", TEST_DB_URL);

  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionProvider);

  auto client = MyClient(executor);

  {
    auto res = client.selectValues();
    if(res->isSuccess()) {
      OATPP_LOGd(TAG, "OK, knownCount={}, hasMore={}", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGd(TAG, "Error, message={}", message->c_str());
    }

    auto dataset = res->fetch<oatpp::Vector<oatpp::Object<Row>>>();

    oatpp::json::ObjectMapper om;
    om.serializerConfig().json.useBeautifier = true;
    om.serializerConfig().mapper.enabledInterpretations = { "postgresql" };

    auto str = om.writeToString(dataset);

    OATPP_LOGd(TAG, "res={}", str->c_str());

    OATPP_ASSERT(dataset->size() == 3);

    {
      auto row = dataset[0];
      OATPP_ASSERT(row->f_enumint == nullptr);
      OATPP_ASSERT(row->f_enumstring == nullptr);
    }

    {
      auto row = dataset[1];
      OATPP_ASSERT(row->f_enumint == Animal::DOG);
      OATPP_ASSERT(row->f_enumstring == Animal::DOG);
    }

    {
      auto row = dataset[2];
      OATPP_ASSERT(row->f_enumint == Animal::CAT);
      OATPP_ASSERT(row->f_enumstring == Animal::CAT);
    }

  }

  {
    auto res = client.deleteValues();
    if (res->isSuccess()) {
      OATPP_LOGd(TAG, "OK, knownCount={}, hasMore={}", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGd(TAG, "Error, message={}", message->c_str());
    }

    OATPP_ASSERT(res->isSuccess());
  }

  {
    auto connection = client.getConnection();
    {
      auto row = Row::createShared();
      row->f_enumint = nullptr;
      row->f_enumstring = nullptr;
      auto res = client.insertValues(row, connection);
      if (res->isSuccess()) {
          OATPP_LOGd(TAG, "OK, knownCount={}, hasMore={}", res->getKnownCount(), res->hasMoreToFetch());
      }
      else {
          auto message = res->getErrorMessage();
          OATPP_LOGd(TAG, "Error, message={}", message->c_str());
      }

      OATPP_ASSERT(res->isSuccess());
    }

    {
      auto row = Row::createShared();
      row->f_enumint = Animal::HORSE;
      row->f_enumstring = Animal::HORSE;
      auto res = client.insertValues(row, connection);
      if (res->isSuccess()) {
          OATPP_LOGd(TAG, "OK, knownCount={}, hasMore={}", res->getKnownCount(), res->hasMoreToFetch());
      }
      else {
          auto message = res->getErrorMessage();
          OATPP_LOGd(TAG, "Error, message={}", message->c_str());
      }

      OATPP_ASSERT(res->isSuccess());
    }
  }

  {
    auto res = client.selectValues();
    if(res->isSuccess()) {
      OATPP_LOGd(TAG, "OK, knownCount={}, hasMore={}", res->getKnownCount(), res->hasMoreToFetch());
    } else {
      auto message = res->getErrorMessage();
      OATPP_LOGd(TAG, "Error, message={}", message->c_str());
    }

    auto dataset = res->fetch<oatpp::Vector<oatpp::Object<Row>>>();

    oatpp::json::ObjectMapper om;
    om.serializerConfig().json.useBeautifier = true;
    om.serializerConfig().mapper.enabledInterpretations = { "postgresql" };

    auto str = om.writeToString(dataset);

    OATPP_LOGd(TAG, "res={}", str->c_str());

    OATPP_ASSERT(dataset->size() == 2);

    {
      auto row = dataset[0];
      OATPP_ASSERT(row->f_enumint == nullptr);
      OATPP_ASSERT(row->f_enumstring == nullptr);
    }

    {
      auto row = dataset[1];
      OATPP_ASSERT(row->f_enumint == Animal::HORSE);
      OATPP_ASSERT(row->f_enumstring == Animal::HORSE);
    }

  }

}

}}}}
