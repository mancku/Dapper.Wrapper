namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;
    using System.Data;
    using System.Runtime.CompilerServices;

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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            dapperWrapper.Update(product, false);

            // Assert
            AssertUpdated(product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            await dapperWrapper.UpdateAsync(product, false);

            // Assert
            AssertUpdated(product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            dapperWrapper.Update(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdated(product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            await dapperWrapper.UpdateAsync(product);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdated(product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            dapperWrapper.Update(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdated(product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            await dapperWrapper.UpdateAsync(product);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdated(product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            var numOfRowsUpdated = dapperWrapper.Execute(
                product.GenerateUpdateStatementWithoutParameters(sqlDialect),
                    null, false, null, CommandType.Text);

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            var numOfRowsUpdated = await dapperWrapper.ExecuteAsync(
                product.GenerateUpdateStatementWithoutParameters(sqlDialect),
                null, false, null, CommandType.Text);

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            var numOfRowsUpdated = dapperWrapper.Execute(
                product.GenerateUpdateStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            var numOfRowsUpdated = await dapperWrapper.ExecuteAsync(
                product.GenerateUpdateStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.CommitChanges();

            // Assert
            AssertUpdatedFromQuery(numOfRowsUpdated, product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            var numOfRowsUpdated = dapperWrapper.Execute(
                product.GenerateUpdateStatementWithoutParameters(sqlDialect),
                    null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdatedFromQuery(numOfRowsUpdated, product, dapperWrapper, originalProductName);
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
            var originalProductName = product.Name;

            // Act
            ModifyProduct(product, sqlDialect);
            var numOfRowsUpdated = await dapperWrapper.ExecuteAsync(
                product.GenerateUpdateStatementWithoutParameters(sqlDialect),
                null, true, null, CommandType.Text);
            dapperWrapper.RollbackChanges();

            // Assert
            AssertNotUpdatedFromQuery(numOfRowsUpdated, product, dapperWrapper, originalProductName);
        }

        private static void AssertUpdated(Product product, DapperWrapper dapperWrapper, string originalProductName)
        {
            var result = RetrieveUpdatedProduct(dapperWrapper, product.ProductID);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var updated = result.Single();
            updated.Name.Should().NotBeEquivalentTo(originalProductName);
            product.Name.Should().BeEquivalentTo(updated.Name);
        }

        private static void AssertNotUpdated(Product product, DapperWrapper dapperWrapper, string originalProductName)
        {
            var result = RetrieveUpdatedProduct(dapperWrapper, product.ProductID);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var notUpdated = result.Single();
            notUpdated.Name.Should().BeEquivalentTo(originalProductName);
            product.Name.Should().NotBeEquivalentTo(notUpdated.Name);
        }

        private static void AssertUpdatedFromQuery(int numOfRowsUpdated, Product product, DapperWrapper dapperWrapper, string originalProductName)
        {
            numOfRowsUpdated.Should().Be(1);

            var result = RetrieveUpdatedProduct(dapperWrapper, product.ProductID);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var updated = result.Single();
            updated.Name.Should().NotBeEquivalentTo(originalProductName);
            product.Name.Should().BeEquivalentTo(updated.Name);
        }

        private static void AssertNotUpdatedFromQuery(int numOfRowsUpdated, Product product, DapperWrapper dapperWrapper, string originalProductName)
        {
            numOfRowsUpdated.Should().Be(1);

            var result = RetrieveUpdatedProduct(dapperWrapper, product.ProductID);
            result.Any().Should().BeTrue();
            result.Count.Should().Be(1);
            var notUpdated = result.Single();
            notUpdated.Name.Should().BeEquivalentTo(originalProductName);
            product.Name.Should().NotBeEquivalentTo(notUpdated.Name);
        }

        private static List<Product> RetrieveUpdatedProduct(DapperWrapper dapperWrapper, int productId)
        {
            FormattableString filter = $"{nameof(Product.ProductID):C} = {productId}";
            return dapperWrapper.FindAsList<Product>(filter);
        }

        private static void ModifyProduct(Product product, SqlDialect sqlDialect, [CallerMemberName] string callerMethod = "")
        {
            product.Name = $"DW-Test-{sqlDialect}-{callerMethod}";
        }
    }
}