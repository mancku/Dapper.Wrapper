namespace Dapper.Wrapper
{
    using FastCrud;
    using Microsoft.Extensions.Configuration;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;

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

        protected IDbTransaction? Transaction;

        protected IDbConnection TransactionalConnection { get; set; }

        protected IDbConnection Connection { get; set; }

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
            this.TransactionalConnection.Dispose();
            this.Connection.Dispose();
        }

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

        public List<TEntity> ExecuteQueryAsList<TEntity>(string query,
            object? parameters = null,
            bool useTransaction = false,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return this.ExecuteQuery<TEntity>(query, parameters, useTransaction, commandTimeout, commandType)
                .ToList();
        }

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

        public async Task<List<TEntity>> QueryMultipleAsync<TEntity>(string query,
            Func<SqlMapper.GridReader, List<TEntity>> queryMultipleResolver, bool useTransaction = false)
        {
            using var multi = await this.GetConnection(useTransaction).QueryMultipleAsync(query);
            return queryMultipleResolver(multi);
        }

        public TEntity ExecuteScalar<TEntity>(string query, object? parameters = null, bool useTransaction = false,
            int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.GetConnection(useTransaction).ExecuteScalar<TEntity>(query,
                parameters,
                this.GetTransaction(useTransaction),
                commandTimeout,
                commandType);
        }

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
