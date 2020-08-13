
#include "oatpp-postgresql/Executor.hpp"

#include "oatpp-test/UnitTest.hpp"

#include "oatpp/core/concurrency/SpinLock.hpp"
#include "oatpp/core/base/Environment.hpp"

#include <iostream>

#include "oatpp/orm/DbClient.hpp"
#include "oatpp/core/macro/codegen.hpp"

#include "oatpp/parser/json/mapping/ObjectMapper.hpp"

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

class Ints : public oatpp::DTO {

  DTO_INIT(Ints, DTO)

  DTO_FIELD(Int8, f_int8);
  DTO_FIELD(UInt8, f_uint8);

  DTO_FIELD(Int16, f_int16);
  DTO_FIELD(UInt16, f_uint16);

  DTO_FIELD(Int32, f_int32);
  DTO_FIELD(UInt32, f_uint32);

  DTO_FIELD(Int64, f_int64);
  DTO_FIELD(UInt64, f_uint64);

};

#include OATPP_CODEGEN_END(DTO)

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::orm::DbClient {
public:

  MyClient(const std::shared_ptr<oatpp::orm::Executor>& executor)
    : oatpp::orm::DbClient(executor)
  {}

  QUERY(getUserById, "SELECT * FROM user WHERE tag=$<text>$a$<text>$ AND userId=:userId AND role=:role",
        PARAM(oatpp::String, userId),
        PARAM(oatpp::String, role))

  QUERY(createUser,
        "INSERT INTO EXAMPLE_USER "
        "(userId, login, password, email) VALUES "
        "(uuid_generate_v4(), :login, :password, :email) "
        "RETURNING *;",
        PARAM(oatpp::String, login),
        PARAM(oatpp::String, password),
        PARAM(oatpp::String, email))

  QUERY(insertStrs,
        "INSERT INTO test_strs "
        "(f_str1, f_str2, f_str3) VALUES "
        "(:f_str1, :f_str2, :f_str3);",
        PARAM(oatpp::String, f_str1),
        PARAM(oatpp::String, f_str2),
        PARAM(oatpp::String, f_str3))

  QUERY(selectStrs, "SELECT * FROM test_strs")

  QUERY(insertInts,
        "INSERT INTO test_ints "
        "(f_int8, f_uint8, f_int16, f_uint16, f_int32, f_uint32, f_int64) VALUES "
        "(:f_int8, :f_uint8, :f_int16, :f_uint16, :f_int32, :f_uint32, :f_int64);",
        PARAM(oatpp::Int8, f_int8), PARAM(oatpp::UInt8, f_uint8),
        PARAM(oatpp::Int16, f_int16), PARAM(oatpp::UInt16, f_uint16),
        PARAM(oatpp::Int32, f_int32), PARAM(oatpp::UInt32, f_uint32),
        PARAM(oatpp::Int64, f_int64))

  QUERY(selectInts, "SELECT * FROM test_ints")

  QUERY(insertFloats,
        "INSERT INTO test_floats "
        "(f_float32, f_float64) VALUES "
        "(:f_float32, :f_float64);",
        PARAM(oatpp::Float32, f_float32), PARAM(oatpp::Float64, f_float64))

  QUERY(selectFloats, "SELECT * FROM test_floats")

};

#include OATPP_CODEGEN_END(DbClient)

class Test : public oatpp::test::UnitTest {
public:
  Test() : oatpp::test::UnitTest("MyTag")
  {}

  void onRun() override {

    oatpp::String connStr = "postgresql://postgres:db-pass@localhost:5432/postgres";
    auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(connStr);
    auto connectionPool = oatpp::postgresql::ConnectionPool::createShared(
      connectionProvider,
      10,
      std::chrono::seconds(1)
    );

    auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionPool);
    auto client = MyClient(executor);

    //client.createUser("my-login1", "pass1", "email@email.com1", connection);
    //client.createUser("my-login2", "pass2", "email@email.com2", connection);

    //client.insertInts(8, 8, 16, 16, 32, 32, 64, connection);
    //client.insertInts(-1, -1, -1, -1, -1, -1, -1, connection);

    //client.insertFloats(0.32, 0.64, connection);
    //client.insertFloats(-0.32, -0.64, connection);

    //client.insertStrs("Hello", "World", "Oat++");
    //client.insertStrs("Hello", "World", "oatpp");
    //client.insertStrs("Yeah", "Ops", "!!!");

    {

      auto res = client.selectStrs();
      OATPP_LOGD(TAG, "OK=%d, count=%d", res->isSuccess(), res->count());

      auto dataset = res->fetch<oatpp::Vector<oatpp::Fields<oatpp::Any>>>();

      oatpp::parser::json::mapping::ObjectMapper om;
      om.getSerializer()->getConfig()->useBeautifier = true;

      auto str = om.writeToString(dataset);

      OATPP_LOGD(TAG, "res=%s", str->c_str());

    }

    connectionPool->stop();

  }

};

void runTests() {
  OATPP_RUN_TEST(Test);
}

}

int main() {

  oatpp::base::Environment::init();

  runTests();

  /* Print how much objects were created during app running, and what have left-probably leaked */
  /* Disable object counting for release builds using '-D OATPP_DISABLE_ENV_OBJECT_COUNTERS' flag for better performance */
  std::cout << "\nEnvironment:\n";
  std::cout << "objectsCount = " << oatpp::base::Environment::getObjectsCount() << "\n";
  std::cout << "objectsCreated = " << oatpp::base::Environment::getObjectsCreated() << "\n\n";

  OATPP_ASSERT(oatpp::base::Environment::getObjectsCount() == 0);

  oatpp::base::Environment::destroy();

  return 0;
}
