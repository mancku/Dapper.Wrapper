# Dapper.Wrapper

Dapper.Wrapper is a fork of the popular [Dapper.FastCrud](https://github.com/MoonStorm/FastCrud) library, designed to streamline database connection and transaction management while addressing a key limitation in handling multiple database types within the same project. This library aims to empower developers to harness the power of Dapper.FastCrud while abstracting the complexities associated with connection and transaction handling, as well as facilitating seamless interaction with various database systems. Dapper.Wrapper retains the efficiency and straightforwardness that Dapper.FastCrud is celebrated for.

## Architecture

Dapper.Wrapper introduces a new solution alongside the original Dapper.FastCrud, comprising a dedicated project targeting [.NET Standard 2.1](https://learn.microsoft.com/en-us/dotnet/standard/net-standard?tabs=net-standard-2-1). By linking all .CS files from `Dapper.FastCrud`, Dapper.Wrapper gains the ability to interact with and modify Dapper.FastCrud's internal classes. This capability is crucial for enhancing functionality and ensuring a cleaner interface for developers.

## Key Features

- **Seamless Connection and Transaction Management**: Dapper.Wrapper automates the intricacies of opening, closing, and managing database transactions, letting developers focus on the core logic.
- **Preservation of Core Functionality**: The original prowess of Dapper.FastCrud remains intact, ensuring you still benefit from its performance and simplicity.
- **Database Type Handling**: Dapper.Wrapper addresses the limitation of Dapper.FastCrud in handling interactions with different types of databases (MSSQL, MySQL and Postgre SQL) within the same project, ensuring consistent and reliable database operations across varied database systems.

## Getting Started

Integrating Dapper.Wrapper into your project is straightforward:

1. Obtain the Dapper.Wrapper repository via cloning or downloading.
2. Incorporate the Dapper.Wrapper project into your solution.
3. Establish a project reference to Dapper.Wrapper in your application.
4. Utilize Dapper.Wrapper within your code, mirroring the usage patterns of Dapper.FastCrud, with the added benefit of enhanced database type handling.

## Enhancements Over Dapper.FastCrud

Dapper.Wrapper introduces modifications to elevate the ease of database connection and transaction management:

1. **Encapsulation**: All public classes from Dapper.FastCrud are marked as internal within Dapper.Wrapper, guiding users to interact with the wrapper classes exclusively.
2. **Simplified Transaction Handling**: The `AttachToTransaction` method is internal, while the new `ShouldUseTransaction` method is public, providing a straightforward mechanism for developers to opt-in or out of transaction usage, and streamlining transaction and connection management for developers.
4. **Modified DapperExtensions**: Modifications in the `Dapper.FastCrud.DapperExtensions.cs` file ensure that transaction objects are appropriately handled and attached based on the `UseTransaction` flag.
5. **Enhanced Constructors**: New constructors in `Dapper.FastCrud.Configuration.StatementOptions.Builders` initialize the `UseTransaction` property, setting sensible defaults based on the builder context.
6. **Robust Database Type Support**: Dapper.Wrapper constructor stores the SQL Dialect. All Dapper.Wrapper methods call to `OverrideStatementDialect` using that Dialect, so every time the Dialect is set accordingly before trying to construct the query.

## License

Dapper.Wrapper is available under the MIT License. For detailed licensing information, please refer to the [LICENSE](LICENSE) file.
