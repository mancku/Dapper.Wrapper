namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;
    using System.Data;

    public class DapperWrapperInsertTests : BaseDapperWrapperTests
    {
        public DapperWrapperInsertTests(TestContainersHandlerFixture fixture)
            : base(fixture)
        {
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void InsertProductNonTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            dapperWrapper.Insert(product, false);

            // Assert
            AssertInserted(product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductNonTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            await dapperWrapper.InsertAsync(product, false);

            // Assert
            AssertInserted(product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void InsertProductTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            dapperWrapper.Insert(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInserted(product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            await dapperWrapper.InsertAsync(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInserted(product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackInsertProduct(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            dapperWrapper.Insert(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotInserted(product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackInsertProductAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            await dapperWrapper.InsertAsync(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotInserted(product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void InsertProductNonTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            var numOfLinesInserted = dapperWrapper.Execute(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                    null, false, null, CommandType.Text);

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductNonTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            var numOfLinesInserted = await dapperWrapper.ExecuteAsync(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                null, false, null, CommandType.Text);

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, minId);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void InsertProductTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            var numOfLinesInserted = dapperWrapper.Execute(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var minId = sqlDialect == SqlDialect.PostgreSql ? 1 : 1000;

            // Act
            var numOfLinesInserted = await dapperWrapper.ExecuteAsync(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, minId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackInsertProductFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            
            // Act
            var numOfLinesInserted = dapperWrapper.Execute(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotInsertedFromQuery(numOfLinesInserted, product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackInsertProductFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var product = Product.FromRandomValues(Faker);
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var numOfLinesInserted = await dapperWrapper.ExecuteAsync(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotInsertedFromQuery(numOfLinesInserted, product, dapperWrapper);
        }

        private static void AssertInserted(Product product, DapperWrapper dapperWrapper, int minId)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            // Assert
            product.ProductID.Should().BeGreaterOrEqualTo(minId);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var inserted = result.Single();
            inserted.ProductID.Should().BeGreaterOrEqualTo(minId);
            inserted.Name.Should().BeEquivalentTo(product.Name);
            inserted.rowguid.Should().NotBeNull();
            inserted.rowguid!.Value.ToString("N").Should().BeEquivalentTo(product.rowguid!.Value.ToString("N"));
        }

        private static void AssertNotInserted(Product product, DapperWrapper dapperWrapper, int minId = 1000)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            // Assert
            product.ProductID.Should().BeGreaterOrEqualTo(minId);
            result.Any().Should().BeFalse();
        }

        private static void AssertInsertedFromQuery(int numOfLinesInserted, Product product, DapperWrapper dapperWrapper, int minId = 1000)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            numOfLinesInserted.Should().Be(1);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var inserted = result.Single();
            inserted.ProductID.Should().BeGreaterOrEqualTo(minId);
            inserted.Name.Should().BeEquivalentTo(product.Name);
            inserted.rowguid.Should().NotBeNull();
            inserted.rowguid!.Value.ToString("N").Should().BeEquivalentTo(product.rowguid!.Value.ToString("N"));
        }

        private static void AssertNotInsertedFromQuery(int numOfLinesInserted, Product product, DapperWrapper dapperWrapper)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            numOfLinesInserted.Should().Be(1);
            result.Any().Should().BeFalse();
        }

        private static List<Product> RetrieveInsertedProduct(DapperWrapper dapperWrapper, Guid? insertedProductGuid)
        {
            FormattableString filter = $"{nameof(Product.rowguid):C} = '{insertedProductGuid!.Value}'";
            return dapperWrapper.FindAsList<Product>(filter);
        }
    }
}