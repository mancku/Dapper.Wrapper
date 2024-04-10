namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;
    using System;
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
        public void UpdateProductNonTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var products = dapperWrapper.GetRandomProducts(Faker);
            var filter = this.GetProductsIdsToUpdate(products, false);
            //var product = new ProductToUpdate(Faker);
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
        public async Task UpdateProductNonTransactionalAsync(SqlDialect sqlDialect)
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
        public void UpdateProductTransactional(SqlDialect sqlDialect)
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
        public async Task UpdateProductTransactionalAsync(SqlDialect sqlDialect)
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
        public void RollbackUpdateProduct(SqlDialect sqlDialect)
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
        public async Task RollbackUpdateProductAsync(SqlDialect sqlDialect)
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
        public void UpdateProductNonTransactionalFromQuery(SqlDialect sqlDialect)
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
        public async Task UpdateProductNonTransactionalFromQueryAsync(SqlDialect sqlDialect)
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
        public void UpdateProductTransactionalFromQuery(SqlDialect sqlDialect)
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
        public async Task UpdateProductTransactionalFromQueryAsync(SqlDialect sqlDialect)
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
        public void RollbackUpdateProductFromQuery(SqlDialect sqlDialect)
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
        public async Task RollbackUpdateProductFromQueryAsync(SqlDialect sqlDialect)
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

        private static void AssertUpdated(FormattableString filter, IReadOnlyCollection<Product> products,
            ProductToUpdate product, int numOfRowsUpdated, DapperWrapper dapperWrapper)
        {
            Assert(filter, products, product, numOfRowsUpdated, dapperWrapper, true);
        }

        private static void AssertNotUpdated(FormattableString filter, IReadOnlyCollection<Product> products,
            ProductToUpdate product, int numOfRowsUpdated, DapperWrapper dapperWrapper)
        {
            Assert(filter, products, product, numOfRowsUpdated, dapperWrapper, false);
        }

        private static void Assert(FormattableString filter, IReadOnlyCollection<Product> products,
            ProductToUpdate product, int numOfRowsUpdated, DapperWrapper dapperWrapper, bool expectedResult)
        {
            var result = dapperWrapper.FindAsList<Product>(filter);

            // Assert
            result.Any().Should().BeTrue();

            result.Count
                .Should().Be(products.Count)
                .And.Be(numOfRowsUpdated);

            result
                .All(x => IsModifiedAsExpected(x, product))
                .Should().Be(expectedResult);
        }

        private static bool IsModifiedAsExpected(Product retrievedProduct, ProductToUpdate modifiedProduct)
        {
            var differenceInSeconds = (retrievedProduct.ModifiedDate - modifiedProduct.ModifiedDate).TotalSeconds;
            var costDifference = Math.Abs(retrievedProduct.StandardCost - modifiedProduct.StandardCost);

            int kk = Math.Min(retrievedProduct.StandardCost.Scale, modifiedProduct.StandardCost.Scale);
            var tolerance = Convert.ToDecimal(1 / Math.Pow(10, kk));

            return (int)differenceInSeconds == 0
                   && (retrievedProduct.ModifiedDate - DateTime.Now).TotalDays > 30
                   && costDifference <= tolerance;
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