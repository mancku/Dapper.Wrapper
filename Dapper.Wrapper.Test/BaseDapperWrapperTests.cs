namespace Dapper.Wrapper.Test
{
    using Bogus;
    using System;

    [Collection("DapperWrapperTestsCollection")]
    public abstract class BaseDapperWrapperTests : IDisposable
    {
        protected DapperWrapper _msSqlDapperWrapper;
        protected DapperWrapper _postgreSqlDapperWrapper;
        protected DapperWrapper _mySqlDapperWrapper;

        private readonly TestContainersHandlerFixture _fixture;
        protected readonly Faker Faker = new();


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
    }
}
