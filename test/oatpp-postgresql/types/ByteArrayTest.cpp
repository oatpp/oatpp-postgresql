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

#include "ByteArrayTest.hpp"

#include "oatpp-postgresql/Types.hpp"
#include "oatpp-postgresql/orm.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"

#include <cstdio>
#include <limits>
#include <vector>

namespace oatpp {
namespace test {
namespace postgresql {
namespace types {

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

class MyByteArrayRow : public oatpp::DTO {

    DTO_INIT(MyByteArrayRow, DTO);

    DTO_FIELD(oatpp::postgresql::ByteArray, f_bytea);
};

#include OATPP_CODEGEN_END(DTO)

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::orm::DbClient {
  public:
    MyClient(const std::shared_ptr<oatpp::orm::Executor> &executor) : oatpp::orm::DbClient(executor) {

        executeQuery("DROP TABLE IF EXISTS oatpp_schema_version_ByteArrayTest;", {});

        oatpp::orm::SchemaMigration migration(executor, "ByteArrayTest");
        migration.addFile(1, TEST_DB_MIGRATION "ByteArrayTest.sql");
        migration.migrate();

        auto version = executor->getSchemaVersion("ByteArrayTest");
        OATPP_LOGD("DbClient", "Migration - OK. Version=%d.", version);
    }

    QUERY(insertBytearrayValues,
          "INSERT INTO test_bytearray "
          "(f_bytea) "
          "VALUES "
          "(:f_bytea);",
          PREPARE(true), PARAM(oatpp::postgresql::ByteArray, f_bytea))

    QUERY(selectAllBytearray, "SELECT f_bytea FROM test_bytearray")
};

#include OATPP_CODEGEN_END(DbClient)

} // namespace

void ByteArrayTest::onRun() {

    OATPP_LOGI(TAG, "DB-URL='%s'", TEST_DB_URL);

    auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
    auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionProvider);
    auto client = MyClient(executor);

    const auto one_element_byte_array = oatpp::postgresql::ByteArray({0xAA});
    const auto two_elements_byte_array = oatpp::postgresql::ByteArray({0xAA, 0xBB});
    const auto three_elements_byte_array = oatpp::postgresql::ByteArray({0xDE, 0xAD, 0xBE});
    const auto four_elements_byte_array = oatpp::postgresql::ByteArray({0xDE, 0xAD, 0xBE, 0xEF});

    {
        auto connection = client.getConnection();

        client.insertBytearrayValues(oatpp::postgresql::ByteArray(), connection);
        client.insertBytearrayValues(one_element_byte_array, connection);
        client.insertBytearrayValues(two_elements_byte_array, connection);
        client.insertBytearrayValues(three_elements_byte_array, connection);
        client.insertBytearrayValues(four_elements_byte_array, connection);
    }

    {
        auto res = client.selectAllBytearray();
        if (res->isSuccess()) {
            OATPP_LOGD(TAG, "OK, knownCount=%d, hasMore=%d", res->getKnownCount(), res->hasMoreToFetch());
        } else {
            auto message = res->getErrorMessage();
            OATPP_LOGD(TAG, "Error, message=%s", message->c_str());
        }

        auto dataset = res->fetch<oatpp::Vector<oatpp::Object<MyByteArrayRow>>>();

        OATPP_ASSERT(dataset->size() == 5);

        {
            const auto &row = dataset[0];
            OATPP_ASSERT(row->f_bytea == oatpp::postgresql::ByteArray());
        }

        {
            const auto &row = dataset[1];

            OATPP_ASSERT(row->f_bytea->size() == one_element_byte_array->size());
            OATPP_ASSERT(*(row->f_bytea) == *(one_element_byte_array));
        }
        {
            const auto &row = dataset[2];

            OATPP_ASSERT(row->f_bytea->size() == two_elements_byte_array->size());
            OATPP_ASSERT(*(row->f_bytea) == *(two_elements_byte_array));
        }
        {
            const auto &row = dataset[3];

            OATPP_ASSERT(row->f_bytea->size() == three_elements_byte_array->size());
            OATPP_ASSERT(*(row->f_bytea) == *(three_elements_byte_array));
        }
        {
            const auto &row = dataset[4];

            OATPP_ASSERT(row->f_bytea->size() == four_elements_byte_array->size());
            OATPP_ASSERT(*(row->f_bytea) == *(four_elements_byte_array));
        }
    }
}

} // namespace types
} // namespace postgresql
} // namespace test
} // namespace oatpp
