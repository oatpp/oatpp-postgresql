
#include "ql_template/ParserTest.hpp"

#include "types/IntTest.hpp"
#include "types/FloatTest.hpp"
#include "types/InterpretationTest.hpp"

#include "oatpp/core/base/Environment.hpp"

namespace {

void runTests() {

  OATPP_RUN_TEST(oatpp::test::postgresql::ql_template::ParserTest);

//  OATPP_RUN_TEST(oatpp::test::postgresql::types::IntTest);
//  OATPP_RUN_TEST(oatpp::test::postgresql::types::FloatTest);
//  OATPP_RUN_TEST(oatpp::test::postgresql::types::InterpretationTest);

}

}

int main() {
  oatpp::base::Environment::init();
  runTests();
  OATPP_ASSERT(oatpp::base::Environment::getObjectsCount() == 0);
  oatpp::base::Environment::destroy();
  return 0;
}
