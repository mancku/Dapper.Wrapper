namespace Dapper.Wrapper
{
    using Dapper.FastCrud;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;

    public partial class DapperWrapper : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="configuration"></param>
        /// <param name="nameOrConnectionString">If the string contains a semicolon (;) it will be considered a connection string.
        /// Otherwise, it will be considered the name of the connection string in the configuration, and will be retrieved from there.</param>
        /// <param name="sqlDialect"></param>
        public DapperWrapper(IConfiguration configuration, string nameOrConnectionString, SqlDialect sqlDialect)
            : this(GetConnectionString(configuration, nameOrConnectionString), sqlDialect)
        {
        }

        public DapperWrapper(string connectionString, SqlDialect sqlDialect)
        {
            OrmConfiguration.DefaultDialect = sqlDialect;

            this.OpenConnection(connectionString, true);
            this.OpenConnection(connectionString, false);
        }

        public IDbTransaction Transaction = null!;
        public IDbConnection TransactionalConnection { get; set; } = null!;
        public IDbConnection Connection { get; set; } = null!;

        public void CommitChanges()
        {
            this.FinishTransaction(true);
        }

        public void RollbackChanges()
        {
            this.FinishTransaction(false);
        }

        public void Dispose()
        {
            this.TransactionalConnection?.Dispose();
            this.Connection?.Dispose();
        }

        public int Execute(string query,
            Dictionary<string, object>? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).Execute(query,
                parameters,
                useTransaction ? this.Transaction : null,
                commandTimeout,
                commandType);
        }

        public async Task<int> ExecuteAsync(string query,
            Dictionary<string, object>? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await this.GetConnection(useTransaction).ExecuteAsync(query,
                parameters,
                useTransaction ? this.Transaction : null,
                commandTimeout,
                commandType);
        }

        public IEnumerable<T> ExecuteQuery<T>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).Query<T>(query,
                parameters,
                useTransaction ? this.Transaction : null,
                false,
                commandTimeout,
                commandType);
        }

        public List<T> ExecuteQueryAsList<T>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.ExecuteQuery<T>(query, parameters, useTransaction, commandTimeout, commandType)
                .ToList();
        }

        public async Task<IEnumerable<T>> ExecuteQueryAsync<T>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await this.GetConnection(useTransaction).QueryAsync<T>(query,
                parameters,
                useTransaction ? this.Transaction : null,
                commandTimeout,
                commandType);
        }

        public async Task<List<T>> ExecuteQueryAsListAsync<T>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            var result =
                await this.ExecuteQueryAsync<T>(query, parameters, useTransaction, commandTimeout, commandType);

            return result.ToList();
        }

        public async Task<List<T>> QueryMultipleAsync<T>(string query,
            Func<SqlMapper.GridReader, List<T>> queryMultipleResolver, bool useTransaction = false)
        {
            using var multi = await this.GetConnection(useTransaction).QueryMultipleAsync(query);
            return queryMultipleResolver(multi);
        }

        public T ExecuteScalar<T>(string query, object? parameters = null, bool useTransaction = false,
            int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).ExecuteScalar<T>(query,
                parameters,
                useTransaction ? this.Transaction : null,
                commandTimeout,
                commandType);
        }

        public async Task<T> ExecuteScalarAsync<T>(string query, object? parameters = null, bool useTransaction = false,
            int? commandTimeout = null, CommandType? commandType = null)
        {
            return await this.GetConnection(useTransaction).ExecuteScalarAsync<T>(query,
                parameters,
                useTransaction ? this.Transaction : null,
                commandTimeout,
                commandType);
        }
    }
}
