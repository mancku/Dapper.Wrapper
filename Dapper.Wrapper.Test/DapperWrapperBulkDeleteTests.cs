namespace Dapper.Wrapper.Test
{
    using SqlManager;
    using FastCrud;
    using FluentAssertions;
    using Models;
    using System.Collections;
    using System.Data;

    public class DapperWrapperBulkDeleteTests : BaseDapperWrapperTests
    {
        public DapperWrapperBulkDeleteTests(TestContainersHandlerFixture fixture)
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
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = dapperWrapper.BulkDelete<Product>(filter, false);

            // Assert
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductNonTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = await dapperWrapper.BulkDeleteAsync<Product>(filter, false);

            // Assert
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = dapperWrapper.BulkDelete<Product>(filter);
            dapperWrapper.CommitChanges();

            // Assert
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);
            // Act
            var result = await dapperWrapper.BulkDeleteAsync<Product>(filter);
            dapperWrapper.CommitChanges();

            // Assert
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackDeleteProduct(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = dapperWrapper.BulkDelete<Product>(filter);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackDeleteProductAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = await dapperWrapper.BulkDeleteAsync<Product>(filter);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductNonTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = GetDeleteQuery(sqlDialect, filter);

            // Act
            var result = dapperWrapper.Execute(query,
                    null, false, null, CommandType.Text);

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductNonTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = GetDeleteQuery(sqlDialect, filter);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, false, null, CommandType.Text);

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertDeleted(filter, products, result, dapperWrapper);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void DeleteProductTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = GetDeleteQuery(sqlDialect, filter);

            // Act
            var result = dapperWrapper.Execute(query,
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = GetDeleteQuery(sqlDialect, filter);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackDeleteProductFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = GetDeleteQuery(sqlDialect, filter);

            // Act
            var result = dapperWrapper.Execute(query,
                    null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertNotDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackDeleteProductFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = GetDeleteQuery(sqlDialect, filter);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertNotDeleted(filter, products, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductsSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var products = sqlManager.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = await sqlManager.DeleteEntitiesAsync(products);

            // Assert
            result.All(x => x.Succeeded).Should().BeTrue();
            AssertDeleted(filter, products, result.Count, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteErrorLogByIdSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            FormattableString order = $"{nameof(ErrorLog.Id):C} ASC";
            var skip = Faker.Random.Number(5, 20);
            var take = Faker.Random.Number(5, 20);
            var errorLogs = sqlManager.FindAsList<ErrorLog>(orderBy: order, skip: skip, top: take);
            var errorLogsIds = errorLogs.Select(x => x.Id);
            FormattableString filter =
                $"{nameof(BaseEntityWithAutoGeneratedId.Id):C} IN ({string.Join(',', errorLogsIds)})";

            // Act
            var result = await sqlManager.BulkDeleteEntitiesByIdAsync(errorLogs);

            // Assert
            result.Should().BeTrue();
            var check = sqlManager.FindAsList<ErrorLog>(filter);
            check.Should().NotBeNull().And.BeEmpty();
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductsFailedSqlManagerAsync(SqlDialect sqlDialect)
        {
            var sqlManager = this.GetSqlManager(sqlDialect);
            var products = sqlManager.GetRandomProducts(Faker, true);
            var filter = this.GetProductsIdsToDelete(products, false);
            List<DbOperationResult<Product>> result = [];
            Exception exception = null!;

            // Act
            try
            {
                result = await sqlManager.DeleteEntitiesAsync(products);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            result.Should().NotBeNull().And.HaveCount(0);
            exception.Should().NotBeNull();
            exception.Message.Should().Contain("-->").And.Contain(nameof(Product));
            AssertNotDeleted(filter, products, products.Count, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task DeleteProductsFailedNoThrowSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var products = sqlManager.GetRandomProducts(Faker, true);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = await sqlManager.DeleteEntitiesAsync(products, throwOnError: false);

            // Assert
            result.Should().NotBeNull().And.HaveCount(products.Count);
            result.Any(x => x.Succeeded).Should().BeFalse();
            AssertNotDeleted(filter, products, result.Count, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackDeleteProductsSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var products = sqlManager.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToDelete(products, false);

            // Act
            var result = await sqlManager.DeleteEntitiesAsync(products, manageTransaction: false);
            sqlManager.RollbackChanges();

            // Assert
            AssertNotDeleted(filter, products, result.Count, sqlManager);
        }

        private static void AssertDeleted(FormattableString filter, ICollection products, int numOfRowsDeleted, DapperWrapper dapperWrapper)
        {
            products.Count.Should().Be(numOfRowsDeleted);

            var result = dapperWrapper.FindAsList<Product>(filter);
            result.Should().NotBeNull().And.BeEmpty();
        }

        private static void AssertNotDeleted(FormattableString filter, IReadOnlyCollection<Product> products, int numOfRowsDeleted, DapperWrapper dapperWrapper)
        {
            var result = dapperWrapper.FindAsList<Product>(filter);

            // Assert
            result.Any().Should().BeTrue();
            result.Should()
                .HaveCount(products.Count)
                .And.HaveCount(numOfRowsDeleted);
            foreach (var product in result)
            {
                products.Any(x => x.ProductID == product.ProductID)
                    .Should().BeTrue();
            }
        }

        private FormattableString GetProductsIdsToDelete(IEnumerable<Product> products, bool addColumnDelimiters)
        {
            var productsIds = products.Select(x => x.ProductID);
            FormattableString columnName = addColumnDelimiters
                ? (FormattableString)$"[{nameof(Product.ProductID):C}]"
                : $"{nameof(Product.ProductID):C}";
            FormattableString filter = $"{columnName} IN ({string.Join(',', productsIds)})";
            return filter;
        }

        private static string GetDeleteQuery(SqlDialect sqlDialect, FormattableString filter)
        {
            return $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);
        }
    }
}