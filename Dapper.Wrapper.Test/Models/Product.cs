namespace Dapper.Wrapper.Test.Models
{
    using Bogus;
    using FastCrud;
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Globalization;

    [Table("Product")]
    public class Product
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int ProductID { get; set; }

        public string Name { get; set; }

        public string ProductNumber { get; set; }

        public string Color { get; set; }

        public decimal StandardCost { get; set; }

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

        public DateTime ModifiedDate { get; set; }

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
            var insertIntoDboProduct = "INSERT INTO [Product] " +
                                       "([Name], [ProductNumber], [Color], [StandardCost], [ListPrice], [Size], [Weight], [ProductCategoryID], [ProductModelID], [SellStartDate], [SellEndDate], [DiscontinuedDate], [ThumbnailPhotoFileName], [rowguid], [ModifiedDate]) " +
                                       "VALUES ('{0}', '{1}', '{2}', {3}, {4}, '{5}', {6}, {7}, {8}, '{9}', '{10}', '{11}', '{12}', '{13}', '{14}')";

            insertIntoDboProduct = sqlDialect switch
            {
                SqlDialect.PostgreSql => insertIntoDboProduct.Replace('[', '"').Replace(']', '"'),
                SqlDialect.MySql => insertIntoDboProduct.Replace('[', '`').Replace(']', '`'),
                _ => insertIntoDboProduct
            };

            return string.Format(
                insertIntoDboProduct,
                this.Name.Replace("'", "''"),
                this.ProductNumber.Replace("'", "''"),
                this.Color.Replace("'", "''"),
                this.StandardCost.ToString(CultureInfo.InvariantCulture),
                this.ListPrice.ToString(CultureInfo.InvariantCulture),
                this.Size is null ? "NULL" : this.Size.Replace("'", "''"),
                this.Weight.HasValue ? this.Weight.Value.ToString(CultureInfo.InvariantCulture) : "NULL",
                this.ProductCategoryID.HasValue ? this.ProductCategoryID.Value.ToString() : "NULL",
                this.ProductModelID.HasValue ? this.ProductModelID.Value.ToString() : "NULL",
                this.SellStartDate.ToString("yyyy-MM-dd HH:mm:ss"),
                this.SellEndDate.HasValue ? this.SellEndDate.Value.ToString("yyyy-MM-dd HH:mm:ss") : "NULL",
                this.DiscontinuedDate.HasValue ? this.DiscontinuedDate.Value.ToString("yyyy-MM-dd HH:mm:ss") : "NULL",
                this.ThumbnailPhotoFileName is null ? "NULL" : this.ThumbnailPhotoFileName.Replace("'", "''"),
                this.rowguid.HasValue ? $"{this.rowguid.Value}" : "NULL",
                this.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss")
            );
        }
    }
}
