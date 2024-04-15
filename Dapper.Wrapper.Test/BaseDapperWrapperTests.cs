namespace Dapper.Wrapper.Test
{
    using Bogus;
    using FastCrud;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;

    [Collection("DapperWrapperTestsCollection")]
    public abstract class BaseDapperWrapperTests : IDisposable
    {
        protected readonly Faker Faker = new();

        private readonly TestContainersHandlerFixture _fixture;
        private readonly DapperWrapper _msSqlDapperWrapper;
        private readonly DapperWrapper _mySqlDapperWrapper;
        private readonly DapperWrapper _postgreSqlDapperWrapper;

        private readonly DapperWrapperSqlManager _msSqlDapperWrapperSqlManager;
        private readonly DapperWrapperSqlManager _mySqlDapperWrapperSqlManager;
        private readonly DapperWrapperSqlManager _postgreSqlDapperWrapperSqlManager;

        protected BaseDapperWrapperTests(TestContainersHandlerFixture fixture)
        {
            _fixture = fixture;
            _msSqlDapperWrapper = new DapperWrapper(_fixture.MsSqlConnectionString, SqlDialect.MsSql);
            _postgreSqlDapperWrapper = new DapperWrapper(_fixture.PostgreSqlConnectionString, SqlDialect.PostgreSql);
            _mySqlDapperWrapper = new DapperWrapper(_fixture.MySqlConnectionString, SqlDialect.MySql);

            var configuration = fixture._configuration;
            var logger = LoggerFactory.Create(builder => { }).CreateLogger("Test");

            _msSqlDapperWrapperSqlManager = new DapperWrapperSqlManager(configuration, logger, _fixture.MsSqlConnectionString, SqlDialect.MsSql);
            _postgreSqlDapperWrapperSqlManager = new DapperWrapperSqlManager(configuration, logger, _fixture.PostgreSqlConnectionString, SqlDialect.PostgreSql);
            _mySqlDapperWrapperSqlManager = new DapperWrapperSqlManager(configuration, logger, _fixture.MySqlConnectionString, SqlDialect.MySql);
        }

        public void Dispose()
        {
            _msSqlDapperWrapper.Dispose();
            _postgreSqlDapperWrapper.Dispose();
            _mySqlDapperWrapper.Dispose();
        }

        protected DapperWrapper GetDapperWrapper(SqlDialect sqlDialect)
        {
            return sqlDialect switch
            {
                SqlDialect.MsSql => _msSqlDapperWrapper,
                SqlDialect.MySql => _mySqlDapperWrapper,
                SqlDialect.PostgreSql => _postgreSqlDapperWrapper,
                _ => throw new ArgumentOutOfRangeException(nameof(sqlDialect), sqlDialect, null)
            };
        }

        protected DapperWrapperSqlManager GetSqlManager(SqlDialect sqlDialect)
        {
            return sqlDialect switch
            {
                SqlDialect.MsSql => _msSqlDapperWrapperSqlManager,
                SqlDialect.MySql => _mySqlDapperWrapperSqlManager,
                SqlDialect.PostgreSql => _postgreSqlDapperWrapperSqlManager,
                _ => throw new ArgumentOutOfRangeException(nameof(sqlDialect), sqlDialect, null)
            };
        }
    }
}