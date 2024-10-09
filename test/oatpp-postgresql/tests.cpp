
#include "ql_template/ParserTest.hpp"

#include "types/ArrayTest.hpp"
#include "types/IntTest.hpp"
#include "types/FloatTest.hpp"
#include "types/InterpretationTest.hpp"
#include "types/CharacterTest.hpp"
#include "types/EnumAsStringTest.hpp"


#include "oatpp-postgresql/orm.hpp"
#include "oatpp/Environment.hpp"

#include <thread>
#include <chrono>

namespace {

void runTests() {

  OATPP_LOGi("Tests", "DB-URL='{}'", TEST_DB_URL);
  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  for(v_int32 i = 0; i < 6; i ++) {
    try {
      auto connection = connectionProvider->get();
      if(connection) {
        OATPP_LOGd("Tests", "Database is up! We've got a connection!");
        break;
      }
    } catch (...) {
      // DO NOTHING
    }

    OATPP_LOGd("Tests", "Database is not ready. Sleep 10s...");
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }

  OATPP_RUN_TEST(oatpp::test::postgresql::ql_template::ParserTest);

  OATPP_RUN_TEST(oatpp::test::postgresql::types::IntTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::FloatTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::ArrayTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::InterpretationTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::CharacterTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::EnumAsStringTest);
}

}

int main() {
  oatpp::Environment::init();
  runTests();
  OATPP_ASSERT(oatpp::Environment::getObjectsCount() == 0);
  oatpp::Environment::destroy();
  return 0;
}
