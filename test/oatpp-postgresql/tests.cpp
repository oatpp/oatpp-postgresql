
#include "ql_template/ParserTest.hpp"

#include "types/ArrayTest.hpp"
#include "types/IntTest.hpp"
#include "types/FloatTest.hpp"
#include "types/InterpretationTest.hpp"


#include "oatpp-postgresql/orm.hpp"
#include "oatpp/core/base/Environment.hpp"

#include <thread>
#include <chrono>

namespace {

void runTests() {

  OATPP_LOGI("Tests", "DB-URL='%s'", TEST_DB_URL);
  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  for(v_int32 i = 0; i < 6; i ++) {
    try {
      auto connection = connectionProvider->get();
      if(connection) {
        OATPP_LOGD("Tests", "Database is up! We've got a connection!");
        break;
      }
    } catch (...) {
      // DO NOTHING
    }

    OATPP_LOGD("Tests", "Database is not ready. Sleep 10s...");
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }

  OATPP_RUN_TEST(oatpp::test::postgresql::ql_template::ParserTest);

  OATPP_RUN_TEST(oatpp::test::postgresql::types::IntTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::FloatTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::ArrayTest);
  OATPP_RUN_TEST(oatpp::test::postgresql::types::InterpretationTest);

}

}

int main() {
  oatpp::base::Environment::init();
  runTests();
  OATPP_ASSERT(oatpp::base::Environment::getObjectsCount() == 0);
  oatpp::base::Environment::destroy();
  return 0;
}
