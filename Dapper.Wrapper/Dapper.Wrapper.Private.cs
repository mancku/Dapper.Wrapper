namespace Dapper.Wrapper
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Linq;
    using Dapper.FastCrud;
    using FastCrud.Configuration.StatementOptions.Builders;
    using Microsoft.Extensions.Configuration;
    using MySqlConnector;
    using Npgsql;

    public partial class DapperWrapper
    {
        private static void EnsureConnectionIsOpen(IDbConnection connection)
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
        }

        private static string GetConnectionString(IConfiguration configuration, string nameOrConnectionString)
        {
            if (nameOrConnectionString.Contains(";"))
            {
                return nameOrConnectionString;
            }

            return configuration.GetConnectionString(nameOrConnectionString) ??
                   throw new ArgumentException(
                       $"Could not find connection string with name '{nameOrConnectionString}'.");
        }

        private IDbConnection GetConnection<T>(Action<IStandardSqlStatementOptionsBuilder<T>>? statementOptions = null)
        {
            var options = new StandardSqlStatementOptionsBuilder<T>();
            statementOptions?.Invoke(options);
            return this.GetConnection(options.UseTransaction);
        }

        private IDbConnection GetConnection(bool isTransactional = false)
        {
            var connection = isTransactional ? this.TransactionalConnection : this.Connection;
            EnsureConnectionIsOpen(connection);
            if (isTransactional && this.Transaction?.Connection == null)
            {
                this.RenewTransaction();
            }

            return connection;
        }

        private void OpenConnection(string connectionString, bool isTransactionalConnection)
        {
            var sqlProviderName = OrmConfiguration.DefaultDialect.ToString();
            if (!DbProviderFactories.GetFactoryClasses().AsEnumerable().Any(x => x.ItemArray.Contains(sqlProviderName)))
            {
                (string providerName, DbProviderFactory instance) factory = OrmConfiguration.DefaultDialect switch
                {
                    SqlDialect.MySql => (sqlProviderName, MySqlConnectorFactory.Instance),
                    SqlDialect.PostgreSql => (sqlProviderName, NpgsqlFactory.Instance),
                    SqlDialect.MsSql => (sqlProviderName, SqlClientFactory.Instance),
                    _ => throw new ArgumentException($"The '{nameof(sqlProviderName)}' is not yet supported.")
                };

                DbProviderFactories.RegisterFactory(factory.providerName, factory.instance);
            }

            var createdConnection = DbProviderFactories.GetFactory(sqlProviderName).CreateConnection();
            if (createdConnection == null)
            {
                throw new Exception(
                    $"DbFactory was unable to create a new connection for the '{sqlProviderName}' provider.");
            }

            createdConnection.ConnectionString = connectionString;
            if (isTransactionalConnection)
            {
                this.TransactionalConnection = createdConnection;
            }
            else
            {
                this.Connection = createdConnection;
            }
        }

        private void RenewTransaction()
        {
            this.Transaction = this.TransactionalConnection.BeginTransaction();
        }

        private void FinishTransaction(bool isCommit)
        {
            if (this.Transaction?.Connection == null)
            {
                return;
            }

            if (isCommit)
            {
                this.Transaction.Commit();
            }
            else
            {
                this.Transaction.Rollback();
            }

            this.RenewTransaction();
        }
    }
}
