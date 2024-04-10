namespace Dapper.Wrapper.Test
{
    using FastCrud;
    using FluentAssertions;
    using Models;

    public class DapperWrapperFetchTests : BaseDapperWrapperTests
    {
        private readonly Dictionary<int, string> _testRecords;
        private readonly FormattableString _inNameFilter;
        private readonly FormattableString _inIdFilter;
        private readonly FormattableString _orderByIdAsc;
        private readonly FormattableString _orderByIdDesc;

        public DapperWrapperFetchTests(TestContainersHandlerFixture fixture)
            : base(fixture)
        {
            _testRecords = new Dictionary<int, string>
            {
                { 680, "HL Road Frame - Black, 58" },
                { 706, "HL Road Frame - Red, 58" },
                { 707, "Sport-100 Helmet, Red" },
            };

            _inNameFilter = $"{nameof(Product.Name):C} IN ('{string.Join("', '", _testRecords.Values)}')";
            _inIdFilter = $"{nameof(Product.ProductID):C} IN ({string.Join(", ", _testRecords.Keys)})";
            _orderByIdAsc = $"{nameof(Product.ProductID):C} ASC";
            _orderByIdDesc = $"{nameof(Product.ProductID):C} DESC";
        }

        #region Find
        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterName(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(_inNameFilter);

            // Assert
            this.AssertFindFilterName(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameWithOptions(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(statement =>
            {
                statement.Where(_inIdFilter);
            });

            // Assert
            this.AssertFindFilterName(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(_inNameFilter);

            // Assert
            this.AssertFindFilterName(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameWithOptionsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(statement =>
            {
                statement.Where(_inIdFilter);
            });

            // Assert
            this.AssertFindFilterName(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameFirstItem(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(_inNameFilter, _orderByIdAsc, top: 1);

            // Assert
            this.AssertFindFilterNameFirstItem(result);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameFirstItemWithOptions(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(statement =>
            {
                statement.Where(_inIdFilter);
                statement.OrderBy(_orderByIdAsc);
                statement.Top(1);
            });

            // Assert
            this.AssertFindFilterNameFirstItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameFirstItemAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(
                _inNameFilter, _orderByIdAsc, top: 1);

            // Assert
            this.AssertFindFilterNameFirstItem(result);
        }


        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameFirstItemWithOptionsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(statement =>
            {
                statement.Where(_inIdFilter);
                statement.OrderBy(_orderByIdAsc);
                statement.Top(1);
            });

            // Assert
            this.AssertFindFilterNameFirstItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameMiddleItem(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(_inNameFilter, _orderByIdAsc, top: 1, skip: 1);

            // Assert
            this.AssertFindFilterNameMiddleItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameMiddleItemWithOptions(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(statement =>
            {
                statement.Where(_inIdFilter);
                statement.OrderBy(_orderByIdAsc);
                statement.Top(1);
                statement.Skip(1);
            });

            // Assert
            this.AssertFindFilterNameMiddleItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameMiddleItemAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(
                _inNameFilter, _orderByIdAsc, top: 1, skip: 1);

            // Assert
            this.AssertFindFilterNameMiddleItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameMiddleItemWithOptionsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(statement =>
            {
                statement.Where(_inIdFilter);
                statement.OrderBy(_orderByIdAsc);
                statement.Top(1);
                statement.Skip(1);
            });

            // Assert
            this.AssertFindFilterNameMiddleItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameLastItem(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(_inNameFilter, _orderByIdDesc, top: 1);

            // Assert
            this.AssertFindFilterNameLastItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void FindFilterNameLastItemWithOptions(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.FindAsList<Product>(statement =>
            {
                statement.Where(_inIdFilter);
                statement.OrderBy(_orderByIdDesc);
                statement.Top(1);
            });

            // Assert
            this.AssertFindFilterNameLastItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameLastItemAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(
                _inNameFilter, _orderByIdDesc, top: 1);

            // Assert
            this.AssertFindFilterNameLastItem(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task FindFilterNameLastItemWithOptionsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.FindAsListAsync<Product>(statement =>
            {
                statement.Where(_inIdFilter);
                statement.OrderBy(_orderByIdDesc);
                statement.Top(1);
            });

            // Assert
            this.AssertFindFilterNameLastItem(result);
        }
        #endregion

        #region Count
        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void CountAll(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.Count<Product>();

            // Assert
            this.AssertCountAll(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void CountAllWithOptions(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.Count<Product>(statement =>
            {
            });

            // Assert
            this.AssertCountAll(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task CountAllAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.CountAsync<Product>();

            // Assert
            this.AssertCountAll(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task CountAllWithOptionsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.CountAsync<Product>(statement =>
            {
            });

            // Assert
            this.AssertCountAll(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void CountSome(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.Count<Product>(_inNameFilter);

            // Assert
            this.AssertCountSome(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void CountSomeWithOptions(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = dapperWrapper.Count<Product>(statement =>
            {
                statement.Where(_inIdFilter);
            });

            // Assert
            this.AssertCountSome(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task CountSomeAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.CountAsync<Product>(_inNameFilter);

            // Assert
            this.AssertCountSome(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task CountSomeWithOptionsAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);

            // Act
            var result = await dapperWrapper.CountAsync<Product>(statement =>
            {
                statement.Where(_inIdFilter);
            });

            // Assert
            this.AssertCountSome(result);
        }
        #endregion

        #region Get
        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void Get(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var id = Faker.PickRandom(_testRecords.Keys.ToList());
            var product = new Product { ProductID = id };

            // Act
            var result = dapperWrapper.Get(product);

            // Assert
            AssertGet(result, id);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task GetAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var id = Faker.PickRandom(_testRecords.Keys.ToList());
            var product = new Product { ProductID = id };

            // Act
            var result = await dapperWrapper.GetAsync(product);

            // Assert
            AssertGet(result, id);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public void NotGet(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = new Product { ProductID = int.MinValue };

            // Act
            var result = dapperWrapper.Get(product);

            // Assert
            AssertNotGet(result);
        }

        [Theory]
        [InlineData(SqlDialect.MsSql)]
        [InlineData(SqlDialect.PostgreSql)]
        [InlineData(SqlDialect.MySql)]
        public async Task NotGetAsync(SqlDialect sqlDialect)
        {
            // Arrange
            var dapperWrapper = this.GetDapperWrapper(sqlDialect);
            var product = new Product { ProductID = int.MinValue };

            // Act
            var result = await dapperWrapper.GetAsync(product);

            // Assert
            AssertNotGet(result);
        }
        #endregion

        private void AssertFindFilterName(IReadOnlyCollection<Product> result)
        {
            if (result == null)
            {
                throw new ArgumentNullException(nameof(result));
            }

            result.Count.Should().Be(_testRecords.Count);
            result.All(x => _testRecords.Keys.Contains(x.ProductID)).Should().BeTrue();
        }

        private void AssertFindFilterNameFirstItem(IReadOnlyCollection<Product> result)
        {
            result.Count.Should().Be(1);
            result.Single().ProductID.Should().Be(_testRecords.Keys.First());
        }

        private void AssertFindFilterNameMiddleItem(IReadOnlyCollection<Product> result)
        {
            result.Count.Should().Be(1);
            result.Single().ProductID.Should().Be(_testRecords.Keys.ElementAt(1));
        }

        private void AssertFindFilterNameLastItem(IReadOnlyCollection<Product> result)
        {
            result.Count.Should().Be(1);
            result.Single().ProductID.Should().Be(_testRecords.Keys.Last());
        }

        private void AssertCountAll(int result)
        {
            result.Should().BeGreaterThan(_testRecords.Count);
        }

        private void AssertCountSome(int result)
        {
            result.Should().Be(_testRecords.Count);
        }

        private static void AssertGet(Product? result, int id)
        {
            result.Should().NotBeNull();
            result!.ProductID.Should().Be(id);
        }

        private static void AssertNotGet(Product? result)
        {
            result.Should().BeNull();
        }
    }
}