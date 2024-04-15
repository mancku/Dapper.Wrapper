namespace Dapper.Wrapper.Test.Models
{
    using Bogus;
    using FastCrud;
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Globalization;
    using System.Text.Json.Serialization;

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

        internal virtual string GenerateUpdateStatementWithoutParameters(SqlDialect sqlDialect)
        {
            var updateProduct = @"UPDATE [Product] SET 
    [StandardCost] = {0},
    [ModifiedDate] = '{1}'
WHERE [ProductID] = {2}
";

            updateProduct = updateProduct.SwitchDialect(sqlDialect);

            return string.Format(
                updateProduct,
                this.StandardCost.ToString(CultureInfo.InvariantCulture),
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

        [JsonIgnore]
        public List<SalesOrderDetail> SalesOrderDetails { get; set; } = [];

        internal static Product FromRandomValues(Faker faker)
        {
            var products = FromRandomValues(1, faker);
            return products.First();
        }

        internal static List<Product> FromRandomValues(int numOfProductsToGenerate, Faker faker)
        {
            var sizes = new List<string>
            {
                "NULL", "38", "40", "42", "44", "46", "48","50", "52", "54", "56",
                "58", "60", "62", "70", "S", "M", "L", "XL", "XXL", "3XL"
            };
            var productFaker = new Faker<Product>()
                    .RuleFor(o => o.ProductID, _ => -1)
                    .RuleFor(o => o.Name, f => f.Commerce.ProductName())
                    .RuleFor(o => o.ProductNumber, f => $"DW-Test-{f.Random.Number(4000)}")
                    .RuleFor(o => o.Color, f => f.Commerce.Color())
                    .RuleFor(o => o.StandardCost, _ => GetDecimalRandomValue())
                    .RuleFor(o => o.Size, f => f.PickRandom(sizes))
                    .RuleFor(o => o.ProductCategoryID, f => f.Random.Number(1, 41))
                    .RuleFor(o => o.ProductModelID, f => f.Random.Number(1, 128))
                    .RuleFor(o => o.SellStartDate, f => f.Date.PastDateOnly().ToDateTime(TimeOnly.MinValue))
                    .RuleFor(o => o.SellEndDate, f => f.Date.SoonDateOnly().ToDateTime(TimeOnly.MinValue))
                    .RuleFor(o => o.DiscontinuedDate, f => f.Date.FutureDateOnly().ToDateTime(TimeOnly.MinValue))
                    .RuleFor(o => o.ThumbNailPhoto, _ => null)
                    .RuleFor(o => o.ThumbnailPhotoFileName, _ => null)
                    .RuleFor(o => o.rowguid, _ => Guid.NewGuid())
                    .RuleFor(o => o.ModifiedDate, _ => DateTime.Now)
                    .UseSeed(DateTime.Now.Microsecond)
                ;

            var products = productFaker.Generate(numOfProductsToGenerate);
            foreach (var result in products)
            {
                result.ListPrice = faker.Random.Decimal(result.StandardCost);
                result.Size = result.Size == "NULL" ? null : result.Size;
                result.Weight = result.Size is null ? null : GetDecimalRandomValue();
            }

            return products;

            decimal GetDecimalRandomValue()
            {
                return Convert.ToDecimal(faker.Random.Number(1, 10000)) / 1000m;
            }
        }

        internal string GenerateInsertStatementWithoutParameters(SqlDialect sqlDialect)
        {
            var insertIntoProduct = @"INSERT INTO [Product] 
([Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) 
VALUES ('{0}', '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, '{9}', {10}, {11}, {12}, {13}, '{14}')";

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

        internal override string GenerateUpdateStatementWithoutParameters(SqlDialect sqlDialect)
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
    }
}
