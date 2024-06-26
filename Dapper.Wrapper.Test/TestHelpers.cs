﻿namespace Dapper.Wrapper.Test
{
    using Bogus;
    using FastCrud;
    using Models;

    internal static class TestHelpers
    {
        internal static string SwitchDialect(this string query, SqlDialect sqlDialect)
        {
            query = sqlDialect switch
            {
                SqlDialect.PostgreSql => query.Replace('[', '"').Replace(']', '"'),
                SqlDialect.MySql => query.Replace('[', '`').Replace(']', '`'),
                _ => query
            };
            return query;
        }

        internal static List<Product> GetRandomProducts(this DapperWrapper dapperWrapper, Faker faker, bool withRelatedSalesOrder = false)
        {
            var products = dapperWrapper.FindAsList<Product>(statement =>
                {
                    statement.ShouldUseTransaction(true);
                    statement.Include<SalesOrderDetail>(join =>
                    {
                        dapperWrapper.OverrideJoinDialect(join);
                        join.LeftOuterJoin();
                    });
                    // By filtering for an ID bigger than 720, we ensure having some clean records
                    // so fetch tests can rely on them
                    FormattableString filter = $"{nameof(Product):T}.{nameof(SalesOrderDetail.ProductID):C} > 720";
                    filter = withRelatedSalesOrder
                        ? (FormattableString)$"{filter} AND {nameof(SalesOrderDetail):T}.{nameof(SalesOrderDetail.ProductID):C} IS NOT NULL"
                        : (FormattableString)$"{filter} AND {nameof(SalesOrderDetail):T}.{nameof(SalesOrderDetail.ProductID):C} IS NULL";
                    statement.Where(filter);
                })
                .ToList();

            var num = faker.Random.Number(2, 5);
            return faker.Random.ListItems(products, num).ToList();
        }

        internal static Product GetRandomProduct(this DapperWrapper dapperWrapper, Faker faker)
        {
            var products = dapperWrapper.GetRandomProducts(faker);
            return products.First();
        }
    }
}
