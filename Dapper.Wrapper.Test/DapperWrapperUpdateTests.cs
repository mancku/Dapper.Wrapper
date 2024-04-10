namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;
    using System.Data;

    public class DapperWrapperUpdateTests : BaseDapperWrapperTests
    {
        public DapperWrapperUpdateTests(TestContainersHandlerFixture fixture)
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
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = dapperWrapper.Update(productToUpdate, false);

            // Assert
            AssertUpdated(product, productToUpdate, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductNonTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = await dapperWrapper.UpdateAsync(productToUpdate, false);

            // Assert
            AssertUpdated(product, productToUpdate, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductTransactional(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = dapperWrapper.Update(productToUpdate);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdated(product, productToUpdate, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = await dapperWrapper.UpdateAsync(productToUpdate);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdated(product, productToUpdate, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackUpdateProduct(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = dapperWrapper.Update(productToUpdate);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdated(product, productToUpdate, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackUpdateProductAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = await dapperWrapper.UpdateAsync(productToUpdate);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdated(product, productToUpdate, result, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductNonTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var numOfRowsUpdated = dapperWrapper.Execute(
                productToUpdate.GenerateUpdateStatementWithoutParameters(sqlDialect),
                    null, false, null, CommandType.Text);

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, productToUpdate, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductNonTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var numOfRowsUpdated = await dapperWrapper.ExecuteAsync(
                productToUpdate.GenerateUpdateStatementWithoutParameters(sqlDialect),
                null, false, null, CommandType.Text);

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, productToUpdate, dapperWrapper);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void UpdateProductTransactionalFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var numOfRowsUpdated = dapperWrapper.Execute(
                productToUpdate.GenerateUpdateStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, productToUpdate, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductTransactionalFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var numOfRowsUpdated = await dapperWrapper.ExecuteAsync(
                productToUpdate.GenerateUpdateStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, productToUpdate, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void RollbackUpdateProductFromQuery(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var numOfRowsUpdated = dapperWrapper.Execute(
                productToUpdate.GenerateUpdateStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdatedFromQuery(numOfRowsUpdated, product, productToUpdate, dapperWrapper);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackUpdateProductFromQueryAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = dapperWrapper.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var numOfRowsUpdated = await dapperWrapper.ExecuteAsync(
                productToUpdate.GenerateUpdateStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdatedFromQuery(numOfRowsUpdated, product, productToUpdate, dapperWrapper);
        }


        private static void AssertUpdated(Product product, ProductToUpdate updatedProduct, bool result, DapperWrapper dapperWrapper)
        {
            result.Should().Be(true);
            Assert(product, updatedProduct, dapperWrapper, true);
        }

        private static void AssertNotUpdated(Product product, ProductToUpdate updatedProduct, bool result, DapperWrapper dapperWrapper)
        {
            result.Should().Be(true);
            Assert(product, updatedProduct, dapperWrapper, false);
        }

        private static void AssertUpdatedFromQuery(int numOfRowsUpdated, Product product, ProductToUpdate updatedProduct, DapperWrapper dapperWrapper)
        {
            var result = numOfRowsUpdated.Equals(1);
            AssertUpdated(product, updatedProduct, result, dapperWrapper);
        }

        private static void AssertNotUpdatedFromQuery(int numOfRowsUpdated, Product product, ProductToUpdate updatedProduct, DapperWrapper dapperWrapper)
        {
            var result = numOfRowsUpdated.Equals(1);
            AssertNotUpdated(product, updatedProduct, result, dapperWrapper);
        }

        private static void Assert(Product product, ProductToUpdate updatedProduct,
            DapperWrapper dapperWrapper, bool expectedResult)
        {

            var entities = RetrieveUpdatedProduct(dapperWrapper, product.ProductID);
            entities.Should().NotBeNullOrEmpty().And.HaveCount(1);
            var entity = entities.Single();

            IsModifiedAsExpected(entity, updatedProduct)
                .Should().Be(expectedResult);
        }

        internal static bool IsModifiedAsExpected(Product retrievedProduct, ProductToUpdate modifiedProduct)
        {
            var differenceInSeconds = (retrievedProduct.ModifiedDate - modifiedProduct.ModifiedDate).TotalSeconds;
            var costDifference = Math.Abs(retrievedProduct.StandardCost - modifiedProduct.StandardCost);

            int kk = Math.Min(retrievedProduct.StandardCost.Scale, modifiedProduct.StandardCost.Scale);
            var tolerance = Convert.ToDecimal(1 / Math.Pow(10, kk));

            return (int)differenceInSeconds == 0
                   && (retrievedProduct.ModifiedDate - DateTime.Now).TotalDays > 30
                   && costDifference <= tolerance;
        }

        private static List<Product> RetrieveUpdatedProduct(DapperWrapper dapperWrapper, int productId)
        {
            FormattableString filter = $"{nameof(Product.ProductID):C} = {productId}";
            return dapperWrapper.FindAsList<Product>(filter);
        }
    }
}