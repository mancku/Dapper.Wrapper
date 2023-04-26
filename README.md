# Dapper.Wrapper

Dapper.Wrapper is a fork of the popular [Dapper.FastCrud](https://github.com/MoonStorm/FastCrud) library, with an emphasis on managing database connections and transactions. This library aims to make it easier for developers to use Dapper.FastCrud without worrying about the intricacies of connections and transactions, while maintaining the simplicity and performance that Dapper.FastCrud is known for.

A new Solution called Dapper.Wrapper has been added besides the Dapper.FastCrud one. A new project called **`Dapper.Wrapper`** has been added as well. This project targets [.NET Standard 2.1](https://learn.microsoft.com/en-us/dotnet/standard/net-standard?tabs=net-standard-2-1) (And only 2.1) and all .CS files from `Dapper.FastCrud` have been added as Link.

Doing so enables Dapper.Wrapper to work with (and modify) the internal classes of Dapper.FastCrud, which is needed to hide the `AttachToTransaction` method.

## Features

- Automatically handles connection opening, closing, and transaction management
- Keeps the original functionality of Dapper.FastCrud intact

## Getting Started

To use Dapper.Wrapper, follow these steps:

1. Clone or download the Dapper.Wrapper repository.

2. Add the Dapper.Wrapper project to your solution.

3. Reference the Dapper.Wrapper project in your application.

4. Start using Dapper.Wrapper in your code, just like you would with Dapper.FastCrud.

## Changes from Dapper.FastCrud

Dapper.Wrapper makes a few changes to the original Dapper.FastCrud library to make it easier to manage database connections and transactions:

1. All public classes from Dapper.FastCrud have been set as internal in Dapper.Wrapper. This change is to ensure that the user interacts only with the wrapper classes and not the internal classes from Dapper.FastCrud.

2. The `AttachToTransaction` method has been set as internal. This modification is to hide the complexities of handling transactions and connections from the end user.

3. A new method called `ShouldUseTransaction` has been added. This method allows users to easily decide if they want to use transactions or not.

4. All public methods of Dapper.FastCrud.DapperExtensions.cs file have been modified to receive the transaction object and to Attach the transaction if UseTransaction is true.

5. Constructors have been added to classes in Dapper.FastCrud.Configuration.StatementOptions.Builders, so the property UseTransaction has a default value. It will be true for `ConditionalBulkSqlStatementOptionsBuilder` and for `StandardSqlStatementOptionsBuilder`, otherwise false.

## License

Dapper.Wrapper is released under the MIT License. Please see the [LICENSE](LICENSE) file for more information.
