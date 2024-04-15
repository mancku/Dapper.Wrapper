namespace Dapper.Wrapper.Test
{
    using SqlManager;
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

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = await sqlManager.UpdateEntityAsync(productToUpdate);

            // Assert
            AssertSqlManagerUpdated(result, product, productToUpdate, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductSqlManagerTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = await sqlManager.UpdateEntityAsync(productToUpdate, manageTransaction: false);
            sqlManager.CommitChanges();

            // Assert
            AssertSqlManagerUpdated(result, product, productToUpdate, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductFailedSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = int.MinValue
            };
            DbOperationResult<ProductToUpdate> result = null!;
            Exception exception = null!;

            // Act
            try
            {
                result = await sqlManager.UpdateEntityAsync(productToUpdate);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            result.Should().BeNull();
            exception.Should().NotBeNull();
            exception.Message.Should().Be("Update result was false but didn't throw exception");
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task UpdateProductFailedNoThrowSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = int.MinValue
            };

            // Act
            var result = await sqlManager.UpdateEntityAsync(productToUpdate, throwOnError: false);

            // Assert
            result.Should().NotBeNull();
            var resultSucceeded = result.Succeeded.Equals(false);
            AssertNotUpdated(product, productToUpdate, resultSucceeded, sqlManager);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackUpdateProductSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = sqlManager.GetRandomProduct(Faker);
            var productToUpdate = new ProductToUpdate(Faker)
            {
                ProductID = product.ProductID
            };

            // Act
            var result = await sqlManager.UpdateEntityAsync(productToUpdate, manageTransaction: false);
            sqlManager.RollbackChanges();

            // Assert
            result.Should().NotBeNull();
            AssertNotUpdated(product, productToUpdate, result.Succeeded, sqlManager);
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

        private static void AssertSqlManagerUpdated(DbOperationResult<ProductToUpdate> result, Product product,
            ProductToUpdate updatedProduct, DapperWrapperSqlManager sqlManager)
        {
            result.Should().NotBeNull();
            AssertUpdated(product, updatedProduct, result.Succeeded, sqlManager);
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

            int numOfDecimals = Math.Min(retrievedProduct.StandardCost.Scale, modifiedProduct.StandardCost.Scale);
            var tolerance = Convert.ToDecimal(1 / Math.Pow(10, numOfDecimals));

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