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

#include "Connection.hpp"

namespace oatpp { namespace postgresql {

Connection::Connection(PGconn* connection)
  : m_connection(connection)
{}

Connection::~Connection() {
  if(m_connection != nullptr) {
    PQfinish(m_connection);
  }
}

PGconn* Connection::getHandle() {
  return m_connection;
}

void Connection::setPrepared(const oatpp::String& statementName) {
  m_prepared.insert(statementName);
}

bool Connection::isPrepared(const oatpp::String& statementName) {
  auto it = m_prepared.find(statementName);
  return it != m_prepared.end();
}

}}
