namespace Dapper.Wrapper.SqlManager
{
    using System.Collections.Generic;
    using System.Linq;

    internal static class ExtensionsAndHelpers
    {
        internal static string? ConvertToSqlIn(this IReadOnlyCollection<int>? values)
        {
            var collection = values?.ToList();
            var isCollectionNullOrEmpty = !(collection?.Any() ?? false);
            return isCollectionNullOrEmpty
                ? null
                : $"IN ({string.Join(',', collection!)})";
        }
    }
}