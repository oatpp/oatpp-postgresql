#include "ContainerTest.hpp"

#include "oatpp-postgresql/orm.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"
#include "oatpp/core/utils/ConversionUtils.hpp"

namespace oatpp { namespace test { namespace postgresql { namespace types {

namespace {

#include OATPP_CODEGEN_BEGIN(DTO)

class User : public oatpp::DTO {

  DTO_INIT(User, DTO)
  
  DTO_FIELD(String, username);
  DTO_FIELD(String, pass);

};

#include OATPP_CODEGEN_END(DTO)

bool operator==(const Object<User>& us1, const Object<User>& us2)
{
  return us1->username == us2->username && us1->pass == us2->pass;
}

#include OATPP_CODEGEN_BEGIN(DbClient)

class MyClient : public oatpp::orm::DbClient {
public:
    
  MyClient(const std::shared_ptr<oatpp::orm::Executor>& executor)
    : oatpp::orm::DbClient(executor)
  {

    executeQuery("DROP TABLE IF EXISTS oatpp_schema_version_ContainerTest;", {});

    oatpp::orm::SchemaMigration migration(executor, "ContainerTest");
    migration.addFile(1, TEST_DB_MIGRATION "ContainerTest.sql");
    migration.migrate();

    auto version = executor->getSchemaVersion("ContainerTest");
    OATPP_LOGD("DbClient", "Migration - OK. Version=%d.", version);

  }
  
  QUERY(insert_all, 
      "INSERT INTO test_user (username,pass) VALUES"
      " %users (:username, :pass)% RETURNING *;",
      PARAM(oatpp::Vector<oatpp::Object<User>>, users), PREPARE(false));
    
  QUERY(update_all, 
      "UPDATE test_user as target SET pass=source.pass FROM (VALUES %users (:username, :pass)%) "
      "as source(username, pass) WHERE target.username = source.username RETURNING target.*;",
      PARAM(oatpp::Vector<oatpp::Object<User>>, users), PREPARE(false));
     
  QUERY(find_all, 
      "SELECT * FROM test_user WHERE username IN (%users%)",
      PARAM(oatpp::Vector<String>, users), PREPARE(false));
       
  QUERY(delete_all, 
      "DELETE FROM test_user WHERE username IN (%users%)",
      PARAM(oatpp::Vector<String>, users), PREPARE(false));
  
};

#include OATPP_CODEGEN_END(DbClient)

}

void ContainerTest::onRun() {

  OATPP_LOGI(TAG, "DB-URL='%s'", TEST_DB_URL);

  auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>(TEST_DB_URL);
  const auto& executor = std::make_shared<oatpp::postgresql::Executor>(connectionProvider);

  auto client = MyClient(executor);

  {
    auto users = Vector<Object<User>>::createShared();
    for(size_t i = 0; i < 3; ++i)
    {
      auto o = Object<User>::createShared();
      o->username = "test_" + utils::conversion::uint64ToStr(i);
      o->pass = "pass_" + utils::conversion::uint64ToStr(i);
      users->push_back(o);
    }
    auto res = client.insert_all(users);
    OATPP_ASSERT(res->isSuccess())
    const auto& data = res->fetch<Vector<Object<User>>>();
    for(size_t i = 0; i < users->size(); ++i)
    {
      OATPP_ASSERT(users[i] == data[i])
    }
    client.executeQuery("DELETE FROM test_user",{});
  }

  {
    auto users = Vector<Object<User>>::createShared();
    for(size_t i = 0; i < 3; ++i)
    {
      auto o = Object<User>::createShared();
      o->username = "test_" + utils::conversion::uint64ToStr(i);
      o->pass = "pass_" + utils::conversion::uint64ToStr(i);
      users->push_back(o);
    }
    auto res = client.insert_all(users);
    OATPP_ASSERT(res->isSuccess())
    
    for(size_t i = 0; i < users->size(); ++i)
    {
      users[i]->pass = "changed_pass_" + utils::conversion::uint64ToStr(i);
    }
    res = client.update_all(users);
    OATPP_ASSERT(res->isSuccess())
    const auto& data = res->fetch<Vector<Object<User>>>();
    for(size_t i = 0; i < users->size(); ++i)
    {
      OATPP_ASSERT(users[i] == data[i])
    }
    client.executeQuery("DELETE FROM test_user",{});
  }

  {
    auto users = Vector<Object<User>>::createShared();
    for(size_t i = 0; i < 3; ++i)
    {
      auto o = Object<User>::createShared();
      o->username = "test_" + utils::conversion::uint64ToStr(i);
      o->pass = "pass_" + utils::conversion::uint64ToStr(i);
      users->push_back(o);
    }
    auto res = client.insert_all(users);
    OATPP_ASSERT(res->isSuccess())

    auto usernames = Vector<String>::createShared();
    for(size_t i = 0; i < users->size(); ++i)
    {
      usernames->push_back(users[i]->username);
    }
    res = client.find_all(usernames);
    OATPP_ASSERT(res->isSuccess())
    const auto& data = res->fetch<Vector<Object<User>>>();
    for(size_t i = 0; i < users->size(); ++i)
    {
      OATPP_ASSERT(users[i] == data[i])
    }
    client.executeQuery("DELETE FROM test_user",{});
  }

  {
    auto users = Vector<Object<User>>::createShared();
    for(size_t i = 0; i < 3; ++i)
    {
      auto o = Object<User>::createShared();
      o->username = "test_" + utils::conversion::uint64ToStr(i);
      o->pass = "pass_" + utils::conversion::uint64ToStr(i);
      users->push_back(o);
    }
    auto res = client.insert_all(users);
    OATPP_ASSERT(res->isSuccess())

    auto usernames = Vector<String>::createShared();
    for(size_t i = 0; i < users->size(); ++i)
    {
      usernames->push_back(users[i]->username);
    }
    res = client.delete_all(usernames);
    OATPP_ASSERT(res->isSuccess())
    const auto& data = res->fetch<Vector<Object<User>>>();
    OATPP_ASSERT(data->empty());
  }

}

}}}}
