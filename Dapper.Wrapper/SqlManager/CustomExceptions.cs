namespace Dapper.Wrapper.SqlManager
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public class CreatingDuplicateEntryException : Exception
    {
        public CreatingDuplicateEntryException(string message)
            : base(message) { }

        public static string MessageFromDuplicateValues(string propertyName, IEnumerable<string> duplicateValues)
        {
            var sb = new StringBuilder(
                $"There's more than one element with the same value in the '{propertyName}' attribute.");
            sb.Append(" ");
            sb.Append($"Values in '{propertyName}' must be unique.");
            sb.Append(" ");
            sb.Append($"The duplicated values are: ['{string.Join("', '", duplicateValues)}']");

            return sb.ToString();
        }
    }

    public class EntityAlreadyExistsException : Exception
    {
        public EntityAlreadyExistsException(string message)
            : base(message) { }

        public static string MessageFromDuplicateValues(string propertyName, IEnumerable<string> duplicateValues)
        {
            var sb = new StringBuilder(
                $"Error trying to create entities that already exists, looking at the '{propertyName}' attribute.");
            sb.Append(" ");
            sb.Append($"Values in '{propertyName}' must be unique.");
            sb.Append(" ");
            sb.Append($"The duplicated values are: ['{string.Join("', '", duplicateValues)}']");

            return sb.ToString();
        }
    }
}
