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

#ifndef oatpp_postgresql_Connection_hpp
#define oatpp_postgresql_Connection_hpp

#include "oatpp/orm/Connection.hpp"
#include "oatpp/core/provider/Pool.hpp"
#include "oatpp/core/Types.hpp"

#include <libpq-fe.h>

namespace oatpp { namespace postgresql {

/**
 * Implementation of &id:oatpp::orm::Connection; for PostgreSQL.
 */
class Connection : public orm::Connection {
public:

  /**
   * Get PostgreSQL native connection handle.
   * @return
   */
  virtual PGconn* getHandle() = 0;

  virtual void setPrepared(const oatpp::String& statementName) = 0;
  virtual bool isPrepared(const oatpp::String& statementName) = 0;

};

class ConnectionImpl : public Connection {
private:
  PGconn* m_connection;
  std::unordered_set<oatpp::String> m_prepared;
public:

  ConnectionImpl(PGconn* connection);
  ~ConnectionImpl();

  PGconn* getHandle() override;

  void setPrepared(const oatpp::String& statementName) override;
  bool isPrepared(const oatpp::String& statementName) override;

};

struct ConnectionAcquisitionProxy : public provider::AcquisitionProxy<Connection, ConnectionAcquisitionProxy> {

  ConnectionAcquisitionProxy(const std::shared_ptr<Connection> &resource,
                             const std::shared_ptr<PoolInstance> &pool)
    : provider::AcquisitionProxy<Connection, ConnectionAcquisitionProxy>(resource, pool)
  {}

  PGconn* getHandle() override {
    return _obj->getHandle();
  }

  void setPrepared(const oatpp::String& statementName) override {
    _obj->setPrepared(statementName);
  }

  bool isPrepared(const oatpp::String& statementName) override {
    return _obj->isPrepared(statementName);
  }

};

}}

#endif //oatpp_postgresql_Connection_hpp
