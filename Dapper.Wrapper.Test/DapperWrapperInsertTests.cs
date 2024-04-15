namespace Dapper.Wrapper.Test
{
    using SqlManager;
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
            var originalProductId = product.ProductID;

            // Act
            dapperWrapper.Insert(product, false);

            // Assert
            AssertInserted(product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            await dapperWrapper.InsertAsync(product, false);

            // Assert
            AssertInserted(product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            dapperWrapper.Insert(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInserted(product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            await dapperWrapper.InsertAsync(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInserted(product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            dapperWrapper.Insert(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotInserted(product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            await dapperWrapper.InsertAsync(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotInserted(product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            var numOfLinesInserted = dapperWrapper.Execute(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                    null, false, null, CommandType.Text);

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            var numOfLinesInserted = await dapperWrapper.ExecuteAsync(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                null, false, null, CommandType.Text);

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            var numOfLinesInserted = dapperWrapper.Execute(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, originalProductId);
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
            var originalProductId = product.ProductID;

            // Act
            var numOfLinesInserted = await dapperWrapper.ExecuteAsync(
                product.GenerateInsertStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertInsertedFromQuery(numOfLinesInserted, product, dapperWrapper, originalProductId);
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

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = Product.FromRandomValues(Faker);
            var originalProductId = product.ProductID;

            // Act
            var result = await sqlManager.InsertEntityAsync(product);

            // Assert
            AssertInsertedSqlManager(result, product, sqlManager, originalProductId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductSqlManagerTransactionalAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = Product.FromRandomValues(Faker);
            var originalProductId = product.ProductID;

            // Act
            var result = await sqlManager.InsertEntityAsync(product, manageTransaction: false);
            sqlManager.CommitChanges();

            // Assert
            AssertInsertedSqlManager(result, product, sqlManager, originalProductId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductFailedSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = Product.FromRandomValues(Faker);
            product.Name = null!;

            DbOperationResult<Product> result = null!;
            Exception exception = null!;

            // Act
            try
            {
                result = await sqlManager.InsertEntityAsync(product);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            // Assert
            result.Should().BeNull();
            exception.Should().NotBeNull();
            exception.Message.ToLowerInvariant().Should()
                .ContainAll("column", nameof(Product.Name).ToLowerInvariant(), "null");
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task InsertProductFailedNoThrowSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = Product.FromRandomValues(Faker);
            product.Name = null!;
            var originalProductId = product.ProductID;

            // Act
            var result = await sqlManager.InsertEntityAsync(product, throwOnError: false);

            // Assert
            result.Should().NotBeNull();
            result.Succeeded.Should().BeFalse();
            AssertNotInsertedSqlManager(result, product, sqlManager, originalProductId);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task RollbackInsertProductSqlManagerAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var sqlManager = this.GetSqlManager(sqlDialect);
            var product = Product.FromRandomValues(Faker);
            var originalProductId = product.ProductID;

            // Act
            var result = await sqlManager.InsertEntityAsync(product, manageTransaction: false);
            sqlManager.RollbackChanges();

            // Assert
            result.Should().NotBeNull();
            result.Succeeded.Should().BeTrue();
            AssertNotInsertedSqlManager(result, product, sqlManager, originalProductId);
        }

        private static void AssertInserted(Product product, DapperWrapper dapperWrapper, int originalProductId)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            // Assert
            result.Should().HaveCount(1);
            var inserted = result.Single();
            inserted.ProductID.Should()
                .BeGreaterThan(originalProductId)
                .And.Be(product.ProductID);
            inserted.Name.Should().BeEquivalentTo(product.Name);
            inserted.rowguid.Should().NotBeNull();
            inserted.rowguid!.Value.ToString("N").Should().BeEquivalentTo(product.rowguid!.Value.ToString("N"));
        }

        private static void AssertNotInserted(Product product, DapperWrapper dapperWrapper, int originalProductId)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            // Assert
            product.ProductID.Should().BeGreaterOrEqualTo(originalProductId);
            result.Any().Should().BeFalse();
        }

        private static void AssertInsertedFromQuery(int numOfLinesInserted, Product product, DapperWrapper dapperWrapper, int originalProductId = 1000)
        {
            var result = RetrieveInsertedProduct(dapperWrapper, product.rowguid);

            numOfLinesInserted.Should().Be(1);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var inserted = result.Single();
            inserted.ProductID.Should().BeGreaterOrEqualTo(originalProductId);
            inserted.Name.Should().BeEquivalentTo(product.Name);
            inserted.rowguid.Should().NotBeNull();
            inserted.rowguid!.Value.ToString("N").Should().BeEquivalentTo(product.rowguid!.Value.ToString("N"));
        }

        private static void AssertInsertedSqlManager(DbOperationResult<Product> result, Product product,
            DapperWrapperSqlManager sqlManager, int originalProductId)
        {
            result.Should().NotBeNull();
            result.Succeeded.Should().BeTrue();
            result.Entity.ProductID.Should().Be(product.ProductID);
            AssertInserted(product, sqlManager, originalProductId);
        }

        private static void AssertNotInsertedSqlManager(DbOperationResult<Product> result, Product product,
            DapperWrapperSqlManager sqlManager, int originalProductId)
        {
            result.Entity.ProductID.Should().Be(product.ProductID);
            AssertNotInserted(product, sqlManager, originalProductId);
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