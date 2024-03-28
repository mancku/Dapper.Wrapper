namespace Dapper.Wrapper
{
    using FastCrud;
    using FastCrud.Configuration.StatementOptions.Builders;
    using Microsoft.Extensions.Configuration;
    using MySqlConnector;
    using Npgsql;
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Linq;

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

            var notFoundMessage = $"Could not find connection string with name '{nameOrConnectionString}'.";
            return configuration.GetConnectionString(nameOrConnectionString)
                   ?? throw new ArgumentException(notFoundMessage);
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
            if (isTransactional && Transaction?.Connection == null)
            {
                this.RenewTransaction();
            }

            return connection;
        }

        private IDbTransaction? GetTransaction(bool useTransaction)
        {
            return useTransaction ? Transaction : null;
        }

        private void OpenConnection(string connectionString, bool isTransactionalConnection)
        {
            var sqlProviderName = OrmConfiguration.DefaultDialect.ToString();
            if (!DbProviderFactories.GetFactoryClasses().AsEnumerable().Any(x => x.ItemArray.Contains(sqlProviderName)))
            {
                DbProviderFactory instance = OrmConfiguration.DefaultDialect switch
                {
                    SqlDialect.MySql => MySqlConnectorFactory.Instance,
                    SqlDialect.PostgreSql => NpgsqlFactory.Instance,
                    SqlDialect.MsSql => SqlClientFactory.Instance,
                    _ => throw new ArgumentException($"The '{nameof(sqlProviderName)}' is not yet supported.")
                };

                DbProviderFactories.RegisterFactory(sqlProviderName, instance);
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
            Transaction = this.TransactionalConnection.BeginTransaction();
        }

        private void FinishTransaction(bool isCommit)
        {
            if (Transaction?.Connection == null)
            {
                return;
            }

            if (isCommit)
            {
                Transaction.Commit();
            }
            else
            {
                Transaction.Rollback();
            }

            this.RenewTransaction();
        }
    }
}
