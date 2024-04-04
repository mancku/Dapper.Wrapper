namespace Dapper.Wrapper
{
    using FastCrud;
    using Microsoft.Extensions.Configuration;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Wrapper for both Dapper.FastCrud and Dapper.
    /// It's designed so to simplify the database operations
    /// while still being able to access the wrapped methods.
    /// </summary>
    public partial class DapperWrapper : IDisposable
    {
        private readonly SqlDialect _sqlDialect;

        /// <summary>
        /// Initializes a new instance of the DapperWrapper class, setting up the necessary configuration for database operations.
        /// </summary>
        /// <param name="configuration">An IConfiguration instance that provides access to application settings, needed for reading the connection string if the name of it is provided.</param>
        /// <param name="nameOrConnectionString">A string that can either be the name of a connection string in the application's configuration or the connection string itself.
        /// If the string contains a semicolon (;) it will be considered a connection string.
        /// Otherwise, it will be considered the name of the connection string in the configuration, and will be retrieved from there.</param>
        /// <param name="sqlDialect">An enumeration value of type SqlDialect that specifies the SQL dialect to be used by Dapper. This ensures that SQL generated or interpreted by Dapper is appropriate for the specific database being targeted (e.g., SQL Server, MySQL, PostgreSQL).</param>
        public DapperWrapper(IConfiguration configuration, string nameOrConnectionString, SqlDialect sqlDialect)
            : this(GetConnectionString(configuration, nameOrConnectionString), sqlDialect)
        {
        }

        /// <summary>
        /// Initializes a new instance of the DapperWrapper class, setting up the necessary configuration for database operations.
        /// </summary>
        /// <param name="connectionString">This string is used to establish a database connection for Dapper operations</param>
        /// <param name="sqlDialect">An enumeration value of type SqlDialect that specifies the SQL dialect to be used by Dapper. This ensures that SQL generated or interpreted by Dapper is appropriate for the specific database being targeted (e.g., SQL Server, MySQL, PostgreSQL).</param>
        public DapperWrapper(string connectionString, SqlDialect sqlDialect)
        {
            OrmConfiguration.DefaultDialect = sqlDialect;
            _sqlDialect = sqlDialect;

            this.OpenConnection(connectionString, true);
            this.OpenConnection(connectionString, false);
        }

        /// <summary>
        /// An IDbTransaction to be used by the TransactionalConnection and in transactional operations.
        /// </summary>
        protected IDbTransaction? Transaction;

        /// <summary>
        /// An IDbConnection that the DapperWrapper class will always have tied to the Transaction.
        /// So, it's the connection to use when performing transactional operations.
        /// </summary>
        protected IDbConnection TransactionalConnection { get; set; } = null!;

        /// <summary>
        /// An IDbConnection that the DapperWrapper class will never have tied to the Transaction.
        /// So, it's the connection to use when performing non-transactional operations.
        /// </summary>
        protected IDbConnection Connection { get; set; } = null!;

        /// <summary>
        /// Commits the Transaction (if any) and renews the Transaction, so it's ready for the next use.
        /// </summary>
        public void CommitChanges()
        {
            this.FinishTransaction(true);
        }

        /// <summary>
        /// Rollbacks the Transaction (if any) and renews the Transaction, so it's ready for the next use.
        /// </summary>
        public void RollbackChanges()
        {
            this.FinishTransaction(false);
        }

        /// <summary>
        /// Disposes both the Connection and the Transactional connection.
        /// </summary>
        public void Dispose()
        {
            this.TransactionalConnection.Dispose();
            this.Connection.Dispose();
        }

        /// <summary>
        /// Executes a non-query command against the database, such as an insert, update, or delete operation, or a stored procedure call.
        /// </summary>
        /// <param name="query">The SQL query or stored procedure name to execute.</param>
        /// <param name="parameters">An optional dictionary of parameters to be passed with the query. Each key-value pair in the dictionary represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>An integer representing the number of rows affected by the execution. This can be useful for confirming that the operation impacted the expected number of records.</returns>
        /// <remarks>
        /// This method provides a flexible way to execute a variety of SQL commands, allowing for parameterized queries, transactional execution, and custom command timeouts.
        /// </remarks>
        public int Execute(string query,
            Dictionary<string, object>? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).Execute(query,
                parameters,
                this.GetTransaction(useTransaction),
                commandTimeout,
                commandType);
        }

        /// <summary>
        /// Asynchronously executes a non-query command against the database, such as an insert, update, or delete operation, or a stored procedure call.
        /// </summary>
        /// <param name="query">The SQL query or stored procedure name to execute.</param>
        /// <param name="parameters">An optional dictionary of parameters to be passed with the query. Each key-value pair in the dictionary represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A task of integer representing the number of rows affected by the execution. This can be useful for confirming that the operation impacted the expected number of records.</returns>
        /// <remarks>
        /// This method provides a flexible way to execute a variety of SQL commands, allowing for parameterized queries, transactional execution, and custom command timeouts.
        /// </remarks>
        public async Task<int> ExecuteAsync(string query,
            Dictionary<string, object>? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await this.GetConnection(useTransaction).ExecuteAsync(query,
                parameters,
                this.GetTransaction(useTransaction),
                commandTimeout,
                commandType);
        }

        /// <summary>
        /// Executes a query against the database and returns the result set as an enumerable collection of TEntity instances.
        /// </summary>
        /// <typeparam name="TEntity">The entity type that the result set will be mapped to. Each row of the result set is transformed into an instance of TEntity.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. This query should be structured to return a result set that can be mapped to the TEntity type.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>An IEnumerable of TEntity representing the collection of entities retrieved from the database. Each entity in the collection corresponds to a row in the query result set.</returns>
        /// <remarks>
        /// This method is useful for retrieving data from the database and working with it in a strongly typed manner. It abstracts the process of command execution, result set retrieval, and data transformation into TEntity instances.
        /// </remarks>
        public IEnumerable<TEntity> ExecuteQuery<TEntity>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).Query<TEntity>(query,
                parameters,
                this.GetTransaction(useTransaction),
                false,
                commandTimeout,
                commandType);
        }

        /// <summary>
        /// Executes a query against the database and returns the result set as a list of TEntity instances.
        /// </summary>
        /// <typeparam name="TEntity">The entity type that the result set will be mapped to. Each row of the result set is transformed into an instance of TEntity.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. This query should be structured to return a result set that can be mapped to the TEntity type.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A List of TEntity representing the collection of entities retrieved from the database. Each entity in the collection corresponds to a row in the query result set.</returns>
        /// <remarks>
        /// This method is useful for retrieving data from the database and working with it in a strongly typed manner. It abstracts the process of command execution, result set retrieval, and data transformation into TEntity instances.
        /// </remarks>
        public List<TEntity> ExecuteQueryAsList<TEntity>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.ExecuteQuery<TEntity>(query, parameters, useTransaction, commandTimeout, commandType)
                .ToList();
        }

        /// <summary>
        /// Asynchronously executes a query against the database and returns the result set as an enumerable collection of TEntity instances.
        /// </summary>
        /// <typeparam name="TEntity">The entity type that the result set will be mapped to. Each row of the result set is transformed into an instance of TEntity.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. This query should be structured to return a result set that can be mapped to the TEntity type.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A Task of IEnumerable of TEntity representing the collection of entities retrieved from the database. Each entity in the collection corresponds to a row in the query result set.</returns>
        /// <remarks>
        /// This method is useful for retrieving data from the database and working with it in a strongly typed manner. It abstracts the process of command execution, result set retrieval, and data transformation into TEntity instances.
        /// </remarks>
        public async Task<IEnumerable<TEntity>> ExecuteQueryAsync<TEntity>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await this.GetConnection(useTransaction).QueryAsync<TEntity>(query,
                parameters,
                this.GetTransaction(useTransaction),
                commandTimeout,
                commandType);
        }

        /// <summary>
        /// Asynchronously executes a query against the database and returns the result set as a list of TEntity instances.
        /// </summary>
        /// <typeparam name="TEntity">The entity type that the result set will be mapped to. Each row of the result set is transformed into an instance of TEntity.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. This query should be structured to return a result set that can be mapped to the TEntity type.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A Task of List of TEntity representing the collection of entities retrieved from the database. Each entity in the collection corresponds to a row in the query result set.</returns>
        /// <remarks>
        /// This method is useful for retrieving data from the database and working with it in a strongly typed manner. It abstracts the process of command execution, result set retrieval, and data transformation into TEntity instances.
        /// </remarks>
        public async Task<List<TEntity>> ExecuteQueryAsListAsync<TEntity>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            var result =
                await this.ExecuteQueryAsync<TEntity>(query, parameters, useTransaction, commandTimeout, commandType);

            return result.ToList();
        }

        /// <summary>
        /// Executes a query that returns multiple result sets and processes them using a provided resolver function.
        /// </summary>
        /// <typeparam name="TEntity">The entity type that the results should be mapped to. This type is used in the resolver function to transform the GridReader's multiple result sets into a list of TEntity.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. This query is expected to return multiple result sets, which are handled by the resolver function.</param>
        /// <param name="queryMultipleResolver">A function that takes a SqlMapper.GridReader and transforms its multiple result sets into a List of TEntity. The GridReader provides sequential access to each result set, allowing the resolver function to process and transform each one.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A List of TEntity containing the transformed entities from the multiple result sets. The exact structure and contents of this list depend on the implementation of the queryMultipleResolver function.</returns>
        /// <remarks>
        /// This method is useful for executing complex queries or stored procedures that return multiple result sets, which need to be processed or transformed into a single list of entities. The method provides flexibility in handling various types of result sets through the custom resolver function.
        /// </remarks>
        public List<TEntity> QueryMultiple<TEntity>(string query,
            Func<SqlMapper.GridReader, List<TEntity>> queryMultipleResolver,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using var multi = this.GetConnection(useTransaction)
                .QueryMultiple(query, parameters, Transaction, commandTimeout, commandType);
            return queryMultipleResolver(multi);
        }

        /// <summary>
        /// Asynchronously executes a query that returns multiple result sets and processes them using a provided resolver function.
        /// </summary>
        /// <typeparam name="TEntity">The entity type that the results should be mapped to. This type is used in the resolver function to transform the GridReader's multiple result sets into a list of TEntity.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. This query is expected to return multiple result sets, which are handled by the resolver function.</param>
        /// <param name="queryMultipleResolver">A function that takes a SqlMapper.GridReader and transforms its multiple result sets into a List of TEntity. The GridReader provides sequential access to each result set, allowing the resolver function to process and transform each one.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A Task of List of TEntity containing the transformed entities from the multiple result sets. The exact structure and contents of this list depend on the implementation of the queryMultipleResolver function.</returns>
        /// <remarks>
        /// This method is useful for executing complex queries or stored procedures that return multiple result sets, which need to be processed or transformed into a single list of entities. The method provides flexibility in handling various types of result sets through the custom resolver function.
        /// </remarks>
        public async Task<List<TEntity>> QueryMultipleAsync<TEntity>(string query,
            Func<SqlMapper.GridReader, List<TEntity>> queryMultipleResolver,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using var multi = await this.GetConnection(useTransaction)
                .QueryMultipleAsync(query, parameters, Transaction, commandTimeout, commandType);
            return queryMultipleResolver(multi);
        }

        /// <summary>
        /// Executes a query and returns the result of the first column of the first row as an instance of type TEntity.
        /// </summary>
        /// <typeparam name="TEntity">The type that the result should be converted to. The method attempts to cast the first column of the first row of the query result to this type.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. The query is expected to return at least one result, as only the first column of the first row will be retrieved.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>An instance of type TEntity, which is the value of the first column of the first row in the result set. If the result is DBNull or the result set is empty, the default value for TEntity is returned.</returns>
        /// <remarks>
        /// This method is for queries that are designed to return a single value, such as the count of records, a maximum value, or a specific scalar value from the database.
        /// </remarks>
        public TEntity ExecuteScalar<TEntity>(string query, object? parameters = null, bool useTransaction = false,
            int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).ExecuteScalar<TEntity>(query,
                parameters,
                this.GetTransaction(useTransaction),
                commandTimeout,
                commandType);
        }

        /// <summary>
        /// Asynchronously executes a query and returns the result of the first column of the first row as an instance of type TEntity.
        /// </summary>
        /// <typeparam name="TEntity">The type that the result should be converted to. The method attempts to cast the first column of the first row of the query result to this type.</typeparam>
        /// <param name="query">The SQL query or stored procedure to execute. The query is expected to return at least one result, as only the first column of the first row will be retrieved.</param>
        /// <param name="parameters">An optional object containing the parameters to be passed with the query. Typically, this is an anonymous object where each property represents a parameter name and its value. If null, the query is executed without parameters.</param>
        /// <param name="useTransaction">A boolean value indicating whether the execution should be performed within a database transaction. If true, the method ensures the execution is transactional, which can be essential for maintaining data integrity and consistency. Defaults to false.</param>
        /// <param name="commandTimeout">An optional integer specifying the maximum number of seconds to wait for the command execution to complete. If null, the default timeout specified by the underlying database connection is used.</param>
        /// <param name="commandType">An optional CommandType value that indicates whether the query is a plain SQL query or a stored procedure. This allows the method to correctly interpret the query text provided. If null, the command type is inferred based on the query text.</param>
        /// <returns>A Task of TEntity, which is the value of the first column of the first row in the result set. If the result is DBNull or the result set is empty, the default value for TEntity is returned.</returns>
        /// <remarks>
        /// This method is for queries that are designed to return a single value, such as the count of records, a maximum value, or a specific scalar value from the database.
        /// </remarks>
        public async Task<TEntity> ExecuteScalarAsync<TEntity>(string query, object? parameters = null, bool useTransaction = false,
            int? commandTimeout = null, CommandType? commandType = null)
        {
            return await this.GetConnection(useTransaction).ExecuteScalarAsync<TEntity>(query,
                parameters,
                this.GetTransaction(useTransaction),
                commandTimeout,
                commandType);
        }
    }
}
