namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;
    using System.Data;

    public class DapperWrapperDeleteTests : BaseDapperWrapperTests
    {
        public DapperWrapperDeleteTests(TestContainersHandlerFixture fixture)
            : base(fixture)
        {
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductNonTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            dapperWrapper.Delete(product, false);

            // Assert
            AssertDeleted(product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductNonTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            await dapperWrapper.DeleteAsync(product, false);

            // Assert
            AssertDeleted(product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            dapperWrapper.Delete(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertDeleted(product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            await dapperWrapper.DeleteAsync(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertDeleted(product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackDeleteProduct(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            dapperWrapper.Delete(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotDeleted(product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackDeleteProductAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            await dapperWrapper.DeleteAsync(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotDeleted(product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductNonTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            var numOfRowsDeleted = dapperWrapper.Execute(
                product.GenerateDeleteStatementWithoutParameters(sqlDialect),
                    null, false, null, CommandType.Text);

            // Assert
            AssertDeletedFromQuery(numOfRowsDeleted, product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductNonTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            var numOfRowsDeleted = await dapperWrapper.ExecuteAsync(
                product.GenerateDeleteStatementWithoutParameters(sqlDialect),
                null, false, null, CommandType.Text);

            // Assert
            AssertDeletedFromQuery(numOfRowsDeleted, product, dapperWrapper);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            var numOfRowsDeleted = dapperWrapper.Execute(
                product.GenerateDeleteStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertDeletedFromQuery(numOfRowsDeleted, product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            var numOfRowsDeleted = await dapperWrapper.ExecuteAsync(
                product.GenerateDeleteStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertDeletedFromQuery(numOfRowsDeleted, product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackDeleteProductFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            var numOfRowsDeleted = dapperWrapper.Execute(
                product.GenerateDeleteStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotDeletedFromQuery(numOfRowsDeleted, product, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackDeleteProductFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);

            // Act
            var numOfRowsDeleted = await dapperWrapper.ExecuteAsync(
                product.GenerateDeleteStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotDeletedFromQuery(numOfRowsDeleted, product, dapperWrapper);
        }

        private static void AssertDeleted(Product product, DapperWrapper dapperWrapper)
        {
            var result = RetrieveDeletedProduct(dapperWrapper, product.ProductID);

            // Assert
            AssertProductId(product);
            result.Any().Should().BeFalse();
        }

        private static void AssertNotDeleted(Product product, DapperWrapper dapperWrapper)
        {
            var result = RetrieveDeletedProduct(dapperWrapper, product.ProductID);

            // Assert
            AssertProductId(product);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var notDeleted = result.Single();
            notDeleted.ProductID.Should().Be(product.ProductID);
        }

        private static void AssertDeletedFromQuery(int numOfRowsDeleted, Product product, DapperWrapper dapperWrapper)
        {
            var result = RetrieveDeletedProduct(dapperWrapper, product.ProductID);

            numOfRowsDeleted.Should().Be(1);
            result.Any().Should().BeFalse();
        }

        private static void AssertNotDeletedFromQuery(int numOfRowsDeleted, Product product, DapperWrapper dapperWrapper)
        {
            var result = RetrieveDeletedProduct(dapperWrapper, product.ProductID);

            // Assert
            numOfRowsDeleted.Should().Be(1);
            AssertProductId(product);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var notDeleted = result.Single();
            notDeleted.ProductID.Should().Be(product.ProductID);
        }

        private static void AssertProductId(Product product)
        {
            product.ProductID.Should().BeInRange(680, 999);
        }

        private static List<Product> RetrieveDeletedProduct(DapperWrapper dapperWrapper, int productId)
        {
            FormattableString filter = $"{nameof(Product.ProductID):C} = {productId}";
            return dapperWrapper.FindAsList<Product>(filter);
        }
    }
}