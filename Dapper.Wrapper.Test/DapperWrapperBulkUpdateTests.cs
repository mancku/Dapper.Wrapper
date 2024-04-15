namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;
    using SqlManager;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;

    public class DapperWrapperBulkUpdateTests : BaseDapperWrapperTests
    {
        public DapperWrapperBulkUpdateTests(TestContainersHandlerFixture fixture)
            : base(fixture)
        {
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductsNonTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            var product = new ProductToUpdate(Faker);

            // Act
            var result = dapperWrapper.BulkUpdate(product, filter, false);

            // Assert
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProducstNonTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            var product = new ProductToUpdate(Faker);

            // Act
            var result = await dapperWrapper.BulkUpdateAsync(product, filter, false);

            // Assert
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductsTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            var product = new ProductToUpdate(Faker);

            // Act
            var result = dapperWrapper.BulkUpdate(product, filter);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            var product = new ProductToUpdate(Faker);

            // Act
            var result = await dapperWrapper.BulkUpdateAsync(product, filter);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackUpdateProducts(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            var product = new ProductToUpdate(Faker);

            // Act
            var result = dapperWrapper.BulkUpdate(product, filter);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackUpdateProductsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            var product = new ProductToUpdate(Faker);

            // Act
            var result = await dapperWrapper.BulkUpdateAsync(product, filter);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductsNonTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, true);
            var product = new ProductToUpdate(Faker);
            var query = GetUpdateQuery(sqlDialect, product, filter);

            // Act
            var result = dapperWrapper.Execute(query,
                    null, false, null, CommandType.Text);

            // Assert
            filter = this.GetProductsIdsToUpdate(products, false);
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsNonTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, true);
            var product = new ProductToUpdate(Faker);
            var query = GetUpdateQuery(sqlDialect, product, filter);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, false, null, CommandType.Text);

            // Assert
            filter = this.GetProductsIdsToUpdate(products, false);
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductsTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, true);
            var product = new ProductToUpdate(Faker);
            var query = GetUpdateQuery(sqlDialect, product, filter);

            // Act
            var result = dapperWrapper.Execute(query,
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            filter = this.GetProductsIdsToUpdate(products, false);
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, true);
            var product = new ProductToUpdate(Faker);
            var query = GetUpdateQuery(sqlDialect, product, filter);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            filter = this.GetProductsIdsToUpdate(products, false);
            AssertUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackUpdateProductsFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, true);
            var product = new ProductToUpdate(Faker);
            var query = GetUpdateQuery(sqlDialect, product, filter);

            // Act
            var result = dapperWrapper.Execute(query,
                    null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            filter = this.GetProductsIdsToUpdate(products, false);
            AssertNotUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackUpdateProductsFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, true);
            var product = new ProductToUpdate(Faker);
            var query = GetUpdateQuery(sqlDialect, product, filter);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            filter = this.GetProductsIdsToUpdate(products, false);
            AssertNotUpdated(filter, products, product, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = new ProductToUpdate(Faker);
            var products = sqlManager.GetRandomProducts(Faker)
                .Select(x =>
                {
                    x.ModifiedDate = product.ModifiedDate;
                    x.StandardCost = product.StandardCost;
                    return x;
                })
                .ToList();
            var filter = this.GetProductsIdsToUpdate(products, false);

            // Act
            var result = await sqlManager.UpdateEntitiesAsync(products);

            // Assert
            AssertSqlManagerUpdated(filter, result, product, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsSqlManagerTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = new ProductToUpdate(Faker);
            var products = sqlManager.GetRandomProducts(Faker)
                .Select(x =>
                {
                    x.ModifiedDate = product.ModifiedDate;
                    x.StandardCost = product.StandardCost;
                    return x;
                })
                .ToList();
            var filter = this.GetProductsIdsToUpdate(products, false);

            // Act
            var result = await sqlManager.UpdateEntitiesAsync(products, manageTransaction: false);
            sqlManager.CommitChanges();

            // Assert
            AssertSqlManagerUpdated(filter, result, product, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsFailedSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var products = sqlManager.GetRandomProducts(Faker)
                .Select(x =>
                {
                    x.Name = null!;
                    return x;
                })
                .ToList();
            List<DbOperationResult<Product>> result = [];
            Exception exception = null!;

            // Act
            try
            {
                result = await sqlManager.UpdateEntitiesAsync(products);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            result.Should().NotBeNull().And.HaveCount(0);
            exception.Should().NotBeNull();
            exception.Message.Should().Contain("-->").And.Contain(nameof(Product));
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductsFailedNoThrowSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = new ProductToUpdate(Faker);
            var products = sqlManager.GetRandomProducts(Faker)
                .Select(x =>
                {
                    x.ModifiedDate = product.ModifiedDate;
                    x.StandardCost = product.StandardCost;
                    x.Name = null!;
                    return x;
                })
                .ToList();
            var filter = this.GetProductsIdsToUpdate(products, false);

            // Act
            var result = await sqlManager.UpdateEntitiesAsync(products, throwOnError: false);

            // Assert
            result.Should().NotBeNull().And.HaveCount(products.Count);
            result.Any(x => x.Succeeded).Should().BeFalse();
            Assert(filter, products, product, result.Count, sqlManager, false);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackUpdateProductsSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = new ProductToUpdate(Faker);
            var products = sqlManager.GetRandomProducts(Faker)
                .Select(x =>
                {
                    x.ModifiedDate = product.ModifiedDate;
                    x.StandardCost = product.StandardCost;
                    return x;
                })
                .ToList();
            var filter = this.GetProductsIdsToUpdate(products, false);

            // Act
            var result = await sqlManager.UpdateEntitiesAsync(products, manageTransaction: false);
            sqlManager.RollbackChanges();

            // Assert
            AssertSqlManagerNotUpdated(filter, result, product, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductModelsSameNameSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var productModels = sqlManager.FindAsList<ProductModel>($"{nameof(ProductModel.Id):C} > 50");
            var fakeName = Faker.Commerce.ProductName();
            productModels = Faker.PickRandom(productModels, 4)
                .Select(x =>
                {
                    x.Name = fakeName;
                    return x;
                })
                .ToList();
            Exception exception = null!;
            List<DbOperationResult<ProductModel>> result = null!;

            // Act
            try
            {
                result = await sqlManager.UpdateEntitiesAsync(productModels);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            result.Should().BeNull();
            exception.Should().NotBeNull();
            exception.Message.Should()
                .Contain("There's more than one element with the same value in the")
                .And.Contain(nameof(ProductModel.Name));
        }

        private static void AssertUpdated(FormattableString filter, IReadOnlyCollection<Product> products,
                ProductToUpdate product, int numOfRowsUpdated, DapperWrapper dapperWrapper)
        {
            Assert(filter, products, product, numOfRowsUpdated, dapperWrapper, true);
        }

        private static void AssertSqlManagerUpdated(FormattableString filter,
            IReadOnlyCollection<DbOperationResult<Product>> result,
            ProductToUpdate product, DapperWrapper dapperWrapper)
        {
            result.All(x => x.Succeeded).Should().BeTrue();
            var products = result.Select(x => x.Entity).ToList();
            Assert(filter, products, product, result.Count, dapperWrapper, true);
        }

        private static void AssertNotUpdated(FormattableString filter, IReadOnlyCollection<Product> products,
            ProductToUpdate product, int numOfRowsUpdated, DapperWrapper dapperWrapper)
        {
            Assert(filter, products, product, numOfRowsUpdated, dapperWrapper, false);
        }

        private static void AssertSqlManagerNotUpdated(FormattableString filter,
            IReadOnlyCollection<DbOperationResult<Product>> result,
            ProductToUpdate product, DapperWrapper dapperWrapper)
        {
            result.All(x => x.Succeeded).Should().BeTrue();
            var products = result.Select(x => x.Entity).ToList();
            Assert(filter, products, product, result.Count, dapperWrapper, false);
        }

        private static void Assert(FormattableString filter, IReadOnlyCollection<Product> products,
            ProductToUpdate product, int numOfRowsUpdated, DapperWrapper dapperWrapper, bool expectedResult)
        {
            var result = dapperWrapper.FindAsList<Product>(filter);

            // Assert
            result.Should()
                .NotBeNullOrEmpty()
                .And.HaveCount(products.Count)
                .And.HaveCount(numOfRowsUpdated);

            result.All(x => DapperWrapperUpdateTests.IsModifiedAsExpected(x, product))
                .Should().Be(expectedResult);
        }

        private FormattableString GetProductsIdsToUpdate(IEnumerable<Product> products, bool addColumnDelimiters)
        {
            var productsIds = products.Select(x => x.ProductID);
            FormattableString columnName = addColumnDelimiters
                ? (FormattableString)$"[{nameof(Product.ProductID):C}]"
                : $"{nameof(Product.ProductID):C}";
            FormattableString filter = $"{columnName} IN ({string.Join(',', productsIds)})";
            return filter;
        }

        private static string GetUpdateQuery(SqlDialect sqlDialect, ProductToUpdate product, FormattableString filter)
        {
            var modifiedDate = product.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss");
            var standardCost = product.StandardCost.ToString(CultureInfo.GetCultureInfo("en-US"));

            return $@"Update [{nameof(Product):T}] SET
[{nameof(ProductToUpdate.ModifiedDate):C}] = '{modifiedDate}',
[{nameof(ProductToUpdate.StandardCost):C}] = {standardCost}
WHERE {filter}".SwitchDialect(sqlDialect);
        }
    }
}