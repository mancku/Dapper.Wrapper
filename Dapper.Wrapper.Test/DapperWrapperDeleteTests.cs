namespace Dapper.Wrapper.Test
{
    using SqlManager;
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

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);

            // Act
            var result = await sqlManager.DeleteEntityAsync(product);

            // Assert
            AssertDeletedSqlManager(result, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductSqlManagerTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);

            // Act
            var result = await sqlManager.DeleteEntityAsync(product, false);
            sqlManager.CommitChanges();

            // Assert
            AssertDeletedSqlManager(result, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductFailedSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);
            product.ProductID = int.MinValue;

            DbOperationResult<Product> result = null!;
            Exception exception = null!;

            // Act
            try
            {
                result = await sqlManager.DeleteEntityAsync(product);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            result.Should().BeNull();
            exception.Should().NotBeNull();
            exception.Message.Should().Be("Delete result was false but didn't throw exception");
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductFailedNoThrowSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);
            product.ProductID = int.MinValue;

            // Act
            var result = await sqlManager.DeleteEntityAsync(product, throwOnError: false);

            // Assert
            result.Should().NotBeNull();
            result.Succeeded.Should().BeFalse();
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackDeleteProductSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);

            // Act
            var result = await sqlManager.DeleteEntityAsync(product, false);
            sqlManager.RollbackChanges();

            // Assert
            AssertNotDeletedSqlManager(result, sqlManager);
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

        private static void AssertDeletedSqlManager(DbOperationResult<Product> result, DapperWrapperSqlManager sqlManager)
        {
            result.Should().NotBeNull();
            result.Succeeded.Should().BeTrue();
            AssertDeleted(result.Entity, sqlManager);
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
            product.ProductID.Should().BeInRange(720, 999);
        }

        private static void AssertNotDeletedSqlManager(DbOperationResult<Product> result, DapperWrapperSqlManager sqlManager)
        {
            result.Should().NotBeNull();
            result.Succeeded.Should().BeTrue();
            AssertNotDeleted(result.Entity, sqlManager);
        }

        private static List<Product> RetrieveDeletedProduct(DapperWrapper dapperWrapper, int productId)
        {
            FormattableString filter = $"{nameof(Product.ProductID):C} = {productId}";
            return dapperWrapper.FindAsList<Product>(filter);
        }
    }
}