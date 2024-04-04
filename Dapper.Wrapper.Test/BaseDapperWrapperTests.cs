namespace Dapper.Wrapper.Test
{
    using Bogus;
    using FastCrud;
    using System;

    [Collection("DapperWrapperTestsCollection")]
    public abstract class BaseDapperWrapperTests : IDisposable
    {
        protected readonly Faker Faker = new();
        
        private readonly DapperWrapper _msSqlDapperWrapper;
        private readonly DapperWrapper _postgreSqlDapperWrapper;
        private readonly DapperWrapper _mySqlDapperWrapper;

        private readonly TestContainersHandlerFixture _fixture;

        protected BaseDapperWrapperTests(TestContainersHandlerFixture fixture)
        {
            _fixture = fixture;
            _msSqlDapperWrapper = new DapperWrapper(_fixture.MsSqlConnectionString, FastCrud.SqlDialect.MsSql);
            _postgreSqlDapperWrapper = new DapperWrapper(_fixture.PostgreSqlConnectionString, FastCrud.SqlDialect.PostgreSql);
            _mySqlDapperWrapper = new DapperWrapper(_fixture.MySqlConnectionString, FastCrud.SqlDialect.MySql);
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
    }
}
