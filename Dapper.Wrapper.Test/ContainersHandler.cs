namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using System.Data;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Testcontainers.MsSql;
    using Testcontainers.MySql;
    using Testcontainers.PostgreSql;

    public sealed class ContainersHandler : IAsyncLifetime
    {
        private const string _dataBaseName = "DapperWrapperTests";
        private readonly MsSqlContainer _msSqlContainer;
        private readonly PostgreSqlContainer _postgreSqlContainer;
        private readonly MySqlContainer _mySqlContainer;
        private const string _password = "AVeryS3curePassw0rd!";
        private const int _mssqlPort = 1499;

        public ContainersHandler()
        {
            _msSqlContainer = new MsSqlBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2019-latest")
                .WithPassword(_password)
                .WithPortBinding(_mssqlPort, 1433)
                .Build();

            _postgreSqlContainer = new PostgreSqlBuilder()
                .WithImage("postgres:15.2-alpine")
                .WithPassword(_password)
                .WithDatabase(_dataBaseName)
                .WithPortBinding(5499, 5432)
                .Build();

            _mySqlContainer = new MySqlBuilder()
                .WithImage("mysql:8.0")
                .WithPassword(_password)
                .WithDatabase(_dataBaseName)
                .WithPortBinding(3399, 3306)
                .Build();

        }

        [Fact]
        public async Task ReadFromMsSqlDatabase()
        {
            var mssqlConnectionString = $"Server={_msSqlContainer.Hostname},{_mssqlPort};Database={_dataBaseName};User Id=sa;Password={_password};TrustServerCertificate=True";
            using (var dapperWrapper = new DapperWrapper(mssqlConnectionString, SqlDialect.MsSql))
            {
                try
                {
                    var result = await dapperWrapper.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM [ProductModelProductDescription];");
                    result.Should().BeGreaterThan(0);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            var postgreConnectionString = _postgreSqlContainer.GetConnectionString();
            using (var dapperWrapper = new DapperWrapper(postgreConnectionString, SqlDialect.PostgreSql))
            {
                var result = await dapperWrapper.ExecuteScalarAsync<int>("SELECT 1;");
                Assert.Equal(1, result);
            }

            var mySqlConnectionString = _mySqlContainer.GetConnectionString();
            using (var dapperWrapper = new DapperWrapper(mySqlConnectionString, SqlDialect.MySql))
            {
                var result = await dapperWrapper.ExecuteScalarAsync<int>("SELECT 1;");
                Assert.Equal(1, result);
            }
        }

        public async Task InitializeAsync()
        {
            await _msSqlContainer.StartAsync();
            await _postgreSqlContainer.StartAsync();
            await _mySqlContainer.StartAsync();

            var projectPath = Directory.GetParent(Environment.CurrentDirectory)?.Parent?.Parent?.FullName;
            if (string.IsNullOrEmpty(projectPath) || !Directory.Exists(projectPath))
            {
                throw new Exception($"Couldn't find path of the project in ${projectPath}");
            }
            var scriptsFolder = Path.Combine(projectPath, "Scripts");

            await this.InitializeMssqlDb(scriptsFolder);
            await this.InitializePostgresDb(scriptsFolder);
        }

        public async Task DisposeAsync()
        {
            await _msSqlContainer.DisposeAsync().AsTask();
            await _postgreSqlContainer.DisposeAsync().AsTask();
            await _mySqlContainer.DisposeAsync().AsTask();
        }

        private async Task InitializeMssqlDb(string scriptsFolder)
        {
            var mssqlInitScript = await File.ReadAllTextAsync(Path.Combine(scriptsFolder, "mssql.sql"));
            var batches = Regex.Split(mssqlInitScript, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)
                .Select(x => x.Trim())
                .Where(b => !string.IsNullOrWhiteSpace(b));

            using (var dapperWrapper = new DapperWrapper(_msSqlContainer.GetConnectionString(), SqlDialect.MsSql))
            {
                foreach (var batch in batches)
                {
                    try
                    {
                        Console.WriteLine(batch);
                        await dapperWrapper.ExecuteAsync(batch, useTransaction: false, commandTimeout: 300, commandType: CommandType.Text);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                        throw;
                    }
                }
            }
        }

        private async Task InitializePostgresDb(string scriptsFolder)
        {
            var postgresInitScript = await File.ReadAllTextAsync(Path.Combine(scriptsFolder, "postgres.sql"));
            using (var dapperWrapper = new DapperWrapper(_postgreSqlContainer.GetConnectionString(), SqlDialect.PostgreSql))
            {
                try
                {
                    await dapperWrapper.ExecuteAsync(postgresInitScript, useTransaction: false, commandTimeout: 300, commandType: CommandType.Text);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    throw;
                }
            }
        }
    }
}
