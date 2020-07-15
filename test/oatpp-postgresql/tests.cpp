
#include "oatpp-postgresql/Executor.hpp"

#include "oatpp-test/UnitTest.hpp"

#include "oatpp/core/concurrency/SpinLock.hpp"
#include "oatpp/core/base/Environment.hpp"

#include <iostream>


#include "oatpp/database/DbClient.hpp"
#include "oatpp/core/macro/codegen.hpp"

namespace {

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::database::DbClient {
public:

  MyClient(const std::shared_ptr<oatpp::database::Executor>& executor)
    : oatpp::database::DbClient(executor)
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

  QUERY(insertInts,
        "INSERT INTO test_ints "
        "(f_int8, f_uint8, f_int16, f_uint16, f_int32, f_uint32, f_int64) VALUES "
        "(:f_int8, :f_uint8, :f_int16, :f_uint16, :f_int32, :f_uint32, :f_int64);",
        PARAM(oatpp::Int8, f_int8), PARAM(oatpp::UInt8, f_uint8),
        PARAM(oatpp::Int16, f_int16), PARAM(oatpp::UInt16, f_uint16),
        PARAM(oatpp::Int32, f_int32), PARAM(oatpp::UInt32, f_uint32),
        PARAM(oatpp::Int64, f_int64))

  QUERY(insertFloats,
        "INSERT INTO test_floats "
        "(f_float32, f_float64) VALUES "
        "(:f_float32, :f_float64);",
        PARAM(oatpp::Float32, f_float32), PARAM(oatpp::Float64, f_float64))

};

#include OATPP_CODEGEN_END(DbClient)

class Test : public oatpp::test::UnitTest {
public:
  Test() : oatpp::test::UnitTest("MyTag")
  {}

  void onRun() override {

    auto executor = std::make_shared<oatpp::postgresql::Executor>();
    auto client = MyClient(executor);
    auto connection = executor->getConnection();

    //client.createUser("my-login1", "pass1", "email@email.com1", connection);
    //client.createUser("my-login2", "pass2", "email@email.com2", connection);

    //client.insertInts(8, 8, 16, 16, 32, 32, 64, connection);
    //client.insertInts(-1, -1, -1, -1, -1, -1, -1, connection);

    client.insertFloats(0.32, 0.64, connection);
    client.insertFloats(-0.32, -0.64, connection);

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
