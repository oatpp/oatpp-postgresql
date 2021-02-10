# oatpp-postgresql [![Build Status](https://dev.azure.com/lganzzzo/lganzzzo/_apis/build/status/oatpp.oatpp-postgresql?branchName=master)](https://dev.azure.com/lganzzzo/lganzzzo/_build/latest?definitionId=31&branchName=master)

Oat++ ORM adapter for PostgreSQL.  
*Note: this **alpha version**, which means that not all PostgreSQL data-types are supported. See the list of [Supported Data Types](#supported-data-types).*

More about Oat++:

- [Oat++ Website](https://oatpp.io/)
- [Oat++ Github Repository](https://github.com/oatpp/oatpp)
- [Oat++ ORM](https://oatpp.io/docs/components/orm/)

## Build And Install

*Note: you need to install the main [oatpp](https://github.com/oatpp/oatpp) module and PostgreSQL dev package first.*

- Clone this repository.
- In the root of the repository run:
   ```bash
   mkdir build && cd build
   cmake ..
   make install
   ```
   
## API

Detailed documentation on Oat++ ORM you can find [here](https://oatpp.io/docs/components/orm/).

### Connect to Database

All you need to start using oatpp ORM with PostgreSQL is to create `oatpp::postgresql::Executor` and provide it to your `DbClient`.

```cpp
#include "db/MyClient.hpp"
#include "oatpp-postgresql/orm.hpp"

class AppComponent {
public:
  
  /**
   * Create DbClient component.
   */
  OATPP_CREATE_COMPONENT(std::shared_ptr<db::MyClient>, myDatabaseClient)([] {
    /* Create database-specific ConnectionProvider */
    auto connectionProvider = std::make_shared<oatpp::postgresql::ConnectionProvider>("<connection-string>");    
  
    /* Create database-specific ConnectionPool */
    auto connectionPool = oatpp::postgresql::ConnectionPool::createShared(connectionProvider, 
                                                                          10 /* max-connections */, 
                                                                          std::chrono::seconds(5) /* connection TTL */);
    
    /* Create database-specific Executor */
    auto executor = std::make_shared<oatpp::postgresql::Executor>(connectionPool);
  
    /* Create MyClient database client */
    return std::make_shared<MyClient>(executor);
  }());

};
```

### Supported Data Types

|Type|Supported|In Array|
|---|:---:|:---:|
|INT2|+|+|
|INT4|+|+|
|INT8|+|+|
|TIMESTAMP|+|+|
|TEXT|+|+|
|VARCHAR|+|+|
|FLOAT4|+|+|
|FLOAT8|+|+|
|BOOL|+|+|
|UUID|+|+|
|**Other Types**|-|-|
