#ifndef oatpp_test_postgresql_types_ContainerTest_hpp
#define oatpp_test_postgresql_types_ContainerTest_hpp

#include "oatpp-test/UnitTest.hpp"

namespace oatpp { namespace test { namespace postgresql { namespace types {

class ContainerTest : public UnitTest {
public:
  ContainerTest() : UnitTest("TEST[postgresql::types::ContainerTest]") {}
  void onRun() override;
};

}}}}

#endif // oatpp_test_postgresql_types_ContainerTest_hpp
