namespace Dapper.Wrapper.Test
{
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
            var products = this.GetRandomProducts(dapperWrapper);
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
            var products = this.GetRandomProducts(dapperWrapper);
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
            var products = this.GetRandomProducts(dapperWrapper);
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
            var products = this.GetRandomProducts(dapperWrapper);
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
            var products = this.GetRandomProducts(dapperWrapper);
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
            var products = this.GetRandomProducts(dapperWrapper);
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
            var products = this.GetRandomProducts(dapperWrapper);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);

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
            var products = this.GetRandomProducts(dapperWrapper);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);

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
            var products = this.GetRandomProducts(dapperWrapper);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);

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
            var products = this.GetRandomProducts(dapperWrapper);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);

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
            var products = this.GetRandomProducts(dapperWrapper);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);

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
            var products = this.GetRandomProducts(dapperWrapper);
            var filter = this.GetProductsIdsToDelete(products, true);
            var query = $"Delete FROM [{nameof(Product):T}] WHERE {filter}".SwitchDialect(sqlDialect);

            // Act
            var result = await dapperWrapper.ExecuteAsync(query,
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            filter = this.GetProductsIdsToDelete(products, false);
            AssertNotDeleted(filter, products, result, dapperWrapper);
        }

        private static void AssertDeleted(FormattableString filter, ICollection products, int numOfRowsDeleted, DapperWrapper dapperWrapper)
        {
            products.Count.Should().Be(numOfRowsDeleted);

            var result = dapperWrapper.FindAsList<Product>(filter);
            result.Any().Should().BeFalse();
        }

        private static void AssertNotDeleted(FormattableString filter, IReadOnlyCollection<Product> products, int numOfRowsDeleted, DapperWrapper dapperWrapper)
        {
            var result = dapperWrapper.FindAsList<Product>(filter);

            // Assert
            result.Any().Should().BeTrue();
            result.Count
                .Should().Be(products.Count)
                .And.Be(numOfRowsDeleted);
            foreach (var product in result)
            {
                products.Any(x => x.ProductID == product.ProductID)
                    .Should().BeTrue();
            }
        }

        //private static void AssertDeletedFromQuery(int numOfRowsDeleted, Product product, DapperWrapper dapperWrapper)
        //{
        //    var result = RetrieveDeletedProduct(dapperWrapper, product.ProductID);

        //    numOfRowsDeleted.Should().Be(1);
        //    result.Any().Should().BeFalse();
        //}

        //private static void AssertNotDeletedFromQuery(int numOfRowsDeleted, Product product, DapperWrapper dapperWrapper)
        //{
        //    var result = RetrieveDeletedProduct(dapperWrapper, product.ProductID);

        //    // Assert
        //    numOfRowsDeleted.Should().Be(1);
        //    AssertProductId(product);
        //    result.Any().Should().BeTrue();
        //    result.Count.Should().Be(1);
        //    var notDeleted = result.Single();
        //    notDeleted.ProductID.Should().Be(product.ProductID);
        //}

        private FormattableString GetProductsIdsToDelete(IEnumerable<Product> products, bool addColumnDelimiters)
        {
            var productsIds = products.Select(x => x.ProductID);
            FormattableString columnName = addColumnDelimiters
                ? (FormattableString)$"[{nameof(Product.ProductID):C}]"
                : $"{nameof(Product.ProductID):C}";
            FormattableString filter = $"{columnName} IN ({string.Join(',', productsIds)})";
            return filter;
        }

        private List<Product> GetRandomProducts(DapperWrapper dapperWrapper)
        {
            var products = dapperWrapper.FindAsList<Product>(statement =>
                {
                    statement.ShouldUseTransaction(true);
                    statement.Include<SalesOrderDetail>(join =>
                    {
                        dapperWrapper.OverrideJoinDialect(join);
                        join.LeftOuterJoin();
                    });
                })
                .Where(x => !x.SalesOrderDetails.Any())
                .ToList();

            var num = Faker.Random.Number(2, 5);
            return Faker.Random.ListItems(products, num).ToList();
        }

        //private static List<Product> RetrieveDeletedProducts(DapperWrapper dapperWrapper, FormattableString filter)
        //{
        //    return dapperWrapper.FindAsList<Product>(filter);
        //}
    }
}