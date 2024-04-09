namespace Dapper.Wrapper.Test.Models
{
    using Bogus;
    using FastCrud;
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Globalization;

    [Table("Product")]
    public class ProductToUpdate
    {
        public ProductToUpdate() { }

        public ProductToUpdate(Faker faker)
        {
            this.ModifiedDate = DateTime.Now.AddMonths(faker.Random.Number(2, 7));
            this.StandardCost = faker.Random.Decimal(3000m, 5000m);
            this.StandardCost = Math.Round(this.StandardCost, 3);
        }

        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int ProductID { get; set; }

        public DateTime ModifiedDate { get; set; }
        public decimal StandardCost { get; set; }
    }

    [Table("Product")]
    public class Product : ProductToUpdate
    {
        public string Name { get; set; }

        public string ProductNumber { get; set; }

        public string? Color { get; set; }

        public decimal ListPrice { get; set; }

        public string? Size { get; set; }

        public decimal? Weight { get; set; }

        public int? ProductCategoryID { get; set; }

        public int? ProductModelID { get; set; }

        public DateTime SellStartDate { get; set; }

        public DateTime? SellEndDate { get; set; }

        public DateTime? DiscontinuedDate { get; set; }

        public byte[]? ThumbNailPhoto { get; set; }

        public string? ThumbnailPhotoFileName { get; set; }

        public Guid? rowguid { get; set; }

        public List<SalesOrderDetail> SalesOrderDetails { get; set; } = [];

        internal static Product FromRandomValues(Faker faker)
        {
            var sizes = new List<string>
            {
                "NULL", "38", "40", "42", "44", "46", "48","50", "52", "54", "56",
                "58", "60", "62", "70", "S", "M", "L", "XL", "XXL", "3XL"
            };
            var result = new Product
            {
                ProductID = -1,
                Name = faker.Commerce.ProductName(),
                ProductNumber = $"DW-Test-{faker.Random.Number(4000)}",
                Color = faker.Commerce.Color(),
                StandardCost = GetDecimalRandomValue(),
                Size = faker.PickRandom(sizes),
                ProductCategoryID = faker.Random.Number(1, 41),
                ProductModelID = faker.Random.Number(1, 128),
                SellStartDate = faker.Date.PastDateOnly().ToDateTime(TimeOnly.MinValue),
                SellEndDate = faker.Date.SoonDateOnly().ToDateTime(TimeOnly.MinValue),
                DiscontinuedDate = faker.Date.FutureDateOnly().ToDateTime(TimeOnly.MinValue),
                ThumbNailPhoto = null,
                ThumbnailPhotoFileName = null,
                rowguid = Guid.NewGuid(),
                ModifiedDate = DateTime.Now
            };
            result.ListPrice = faker.Random.Decimal(result.StandardCost);
            result.Size = result.Size == "NULL" ? null : result.Size;
            result.Weight = result.Size is null ? null : GetDecimalRandomValue();
            return result;

            decimal GetDecimalRandomValue()
            {
                return Convert.ToDecimal(faker.Random.Number(10000)) / 1000m;
            }
        }

        internal string GenerateInsertStatementWithoutParameters(SqlDialect sqlDialect)
        {
            var insertIntoProduct = "INSERT INTO [Product] " +
                                       "([Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) " +
                                       "VALUES ('{0}', '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, '{9}', {10}, {11}, {12}, {13}, '{14}')";

            insertIntoProduct = insertIntoProduct.SwitchDialect(sqlDialect);

            return string.Format(
                insertIntoProduct,
                this.Name.Replace("'", "''"),
                this.ProductNumber.Replace("'", "''"),
                !string.IsNullOrEmpty(this.Color) ? $"'{this.Color.Replace("'", "''")}'" : "NULL",
                this.StandardCost.ToString(CultureInfo.InvariantCulture),
                this.ListPrice.ToString(CultureInfo.InvariantCulture),
                !string.IsNullOrEmpty(this.Size) ? $"'{this.Size.Replace("'", "''")}'" : "NULL",
                this.Weight.HasValue ? this.Weight.Value.ToString(CultureInfo.InvariantCulture) : "NULL",
                this.ProductCategoryID.HasValue ? this.ProductCategoryID.Value.ToString() : "NULL",
                this.ProductModelID.HasValue ? this.ProductModelID.Value.ToString() : "NULL",
                this.SellStartDate.ToString("yyyy-MM-dd HH:mm:ss"),
                this.SellEndDate.HasValue ? $"'{this.SellEndDate.Value:yyyy-MM-dd HH:mm:ss}'" : "NULL",
                this.DiscontinuedDate.HasValue ? $"'{this.DiscontinuedDate.Value:yyyy-MM-dd HH:mm:ss}'" : "NULL",
                !string.IsNullOrEmpty(this.ThumbnailPhotoFileName) ? $"'{this.ThumbnailPhotoFileName.Replace("'", "''")}'" : "NULL",
                this.rowguid.HasValue ? $"'{this.rowguid.Value}'" : "NULL",
                this.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss")
            );
        }

        internal string GenerateUpdateStatementWithoutParameters(SqlDialect sqlDialect)
        {
            var updateProduct = @"UPDATE [Product] SET 
    [Name] = '{0}',
    [ProductNumber] = '{1}',
    [Color] = {2},
    [StandardCost] = {3},
    [ListPrice] = {4},
    [Size] = {5},
    [Weight] = {6},
    [ProductCategoryID] = {7},
    [ProductModelID] = {8},
    [SellStartDate] = '{9}',
    [SellEndDate] = {10},
    [DiscontinuedDate] = {11},
    [ThumbnailPhotoFileName] = {12},
    [rowguid] = {13},
    [ModifiedDate] = '{14}'
WHERE [ProductID] = {15}
";

            updateProduct = updateProduct.SwitchDialect(sqlDialect);

            return string.Format(
                updateProduct,
                this.Name.Replace("'", "''"),
                this.ProductNumber.Replace("'", "''"),
                !string.IsNullOrEmpty(this.Color) ? $"'{this.Color.Replace("'", "''")}'" : "NULL",
                this.StandardCost.ToString(CultureInfo.InvariantCulture),
                this.ListPrice.ToString(CultureInfo.InvariantCulture),
                !string.IsNullOrEmpty(this.Size) ? $"'{this.Size.Replace("'", "''")}'" : "NULL",
                this.Weight.HasValue ? this.Weight.Value.ToString(CultureInfo.InvariantCulture) : "NULL",
                this.ProductCategoryID.HasValue ? this.ProductCategoryID.Value.ToString() : "NULL",
                this.ProductModelID.HasValue ? this.ProductModelID.Value.ToString() : "NULL",
                this.SellStartDate.ToString("yyyy-MM-dd HH:mm:ss"),
                this.SellEndDate.HasValue ? $"'{this.SellEndDate.Value:yyyy-MM-dd HH:mm:ss}'" : "NULL",
                this.DiscontinuedDate.HasValue ? $"'{this.DiscontinuedDate.Value:yyyy-MM-dd HH:mm:ss}'" : "NULL",
                !string.IsNullOrEmpty(this.ThumbnailPhotoFileName) ? $"'{this.ThumbnailPhotoFileName.Replace("'", "''")}'" : "NULL",
                this.rowguid.HasValue ? $"'{this.rowguid.Value}'" : "NULL",
                this.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss"),
                this.ProductID
            );
        }

        internal string GenerateDeleteStatementWithoutParameters(SqlDialect sqlDialect)
        {
            var deleteStatement = $"DELETE FROM [Product] WHERE [ProductID] = {this.ProductID}";
            return deleteStatement.SwitchDialect(sqlDialect);
        }
    }
}
