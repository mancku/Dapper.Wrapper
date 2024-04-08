namespace Dapper.Wrapper.Test
{
    using FastCrud;

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
    }
}
