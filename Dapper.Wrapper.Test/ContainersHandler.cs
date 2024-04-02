namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Microsoft.Extensions.Configuration;
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
        private readonly IConfiguration _configuration;


        public ContainersHandler()
        {
            _configuration = this.InitializeConfiguration();
            var reUseTestContainers = _configuration.GetValue<bool>("ReUseTestContainers");

            _msSqlContainer = new MsSqlBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2019-latest")
                .WithPassword(_password)
                .WithPortBinding(_mssqlPort, 1433)
                .WithLabel("resource-id", "DapperWrapperTestMsSql")
                .WithReuse(reUseTestContainers)
                .Build();

            _postgreSqlContainer = new PostgreSqlBuilder()
                .WithImage("postgres:15.2-alpine")
                .WithPassword(_password)
                .WithDatabase(_dataBaseName)
                .WithPortBinding(5499, 5432)
                .WithLabel("resource-id", "DapperWrapperTestPostgresSql")
                .WithReuse(reUseTestContainers)
                .Build();

            _mySqlContainer = new MySqlBuilder()
                .WithImage("mysql:8.0")
                .WithPassword(_password)
                .WithDatabase(_dataBaseName)
                .WithPortBinding(3399, 3306)
                .WithLabel("resource-id", "DapperWrapperTestMySql")
                .WithReuse(reUseTestContainers)
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
                    var result = await dapperWrapper.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM ProductModelProductDescription;");
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
                try
                {
                    var result = await dapperWrapper.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM ProductModelProductDescription;");
                    result.Should().BeGreaterThan(0);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            var mySqlConnectionString = _mySqlContainer.GetConnectionString();
            using (var dapperWrapper = new DapperWrapper(mySqlConnectionString, SqlDialect.MySql))
            {
                try
                {
                    var result = await dapperWrapper.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM ProductModelProductDescription;");
                    result.Should().BeGreaterThan(0);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
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
            await this.InitializeMysqlDb(scriptsFolder);
        }

        public async Task DisposeAsync()
        {
            await _msSqlContainer.DisposeAsync().AsTask();
            await _postgreSqlContainer.DisposeAsync().AsTask();
            await _mySqlContainer.DisposeAsync().AsTask();
        }

        private async Task InitializeMssqlDb(string scriptsFolder)
        {
            var seedScript = await File.ReadAllTextAsync(Path.Combine(scriptsFolder, "mssql - seed.sql"));
            var seedBatches = SplitMssqlScriptInBatches(seedScript);
            using (var dapperWrapper = new DapperWrapper(_msSqlContainer.GetConnectionString(), SqlDialect.MsSql))
            {
                var batches = await GetMssqlScriptBatches(scriptsFolder, dapperWrapper, seedBatches);
                await ExecuteMssqlScriptBatches(batches, dapperWrapper);
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

        private async Task InitializeMysqlDb(string scriptsFolder)
        {
            var mysqlInitScript = await File.ReadAllTextAsync(Path.Combine(scriptsFolder, "mysql.sql"));
            using (var dapperWrapper = new DapperWrapper(_mySqlContainer.GetConnectionString(), SqlDialect.MySql))
            {
                try
                {
                    await dapperWrapper.ExecuteAsync(mysqlInitScript, useTransaction: false, commandTimeout: 300, commandType: CommandType.Text);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    throw;
                }
            }
        }

        private static async Task<bool> DoesDatabaseExists(DapperWrapper dapperWrapper, string command)
        {
            var result = await dapperWrapper.ExecuteQueryAsync<int>(command);
            return result.Any();
        }

        private static IEnumerable<string> SplitMssqlScriptInBatches(string script)
        {
            return Regex.Split(script, @"^\s*GO\s*$",
                    RegexOptions.Multiline | RegexOptions.IgnoreCase)
                .Select(x => x.Trim())
                .Where(b => !string.IsNullOrWhiteSpace(b));
        }

        private static async Task ExecuteMssqlScriptBatches(List<string> batches, DapperWrapper dapperWrapper)
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

        private static async Task<List<string>> GetMssqlScriptBatches(string scriptsFolder, DapperWrapper dapperWrapper,
            IEnumerable<string> seedBatches)
        {
            var batches = new List<string>();
            const string command = $"SELECT 1 FROM sys.databases WHERE name = '{_dataBaseName}';";
            var existsDatabase = await DoesDatabaseExists(dapperWrapper, command);
            if (!existsDatabase)
            {
                var createScript = await File.ReadAllTextAsync(Path.Combine(scriptsFolder, "mssql - create.sql"));
                var crateBatches = SplitMssqlScriptInBatches(createScript);
                batches.AddRange(crateBatches);
            }
            batches.AddRange(seedBatches);
            return batches;
        }

        private IConfiguration InitializeConfiguration()
        {
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                //.AddJsonFile("appsettings.json", optional: false)
                //.AddJsonFile("appsettings.Development.json", false)
                .AddJsonFile("appsettings.Test.json", false);

            return configBuilder.Build();
        }
    }
}
