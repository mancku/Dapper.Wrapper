namespace Dapper.Wrapper.SqlManager
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    internal abstract class CommonException
    {
        internal static string MessageFromDuplicateValues(string propertyName, IEnumerable<string> duplicateValues)
        {
            var sb = new StringBuilder($"Values in '{propertyName}' must be unique.");
            sb.Append(" ");
            sb.Append($"The duplicated values are: ['{string.Join("', '", duplicateValues)}']");

            return sb.ToString();
        }
    }


    /// <summary>
    /// Exception for signaling that a creation operation failed due to the existence of duplicate entries.
    /// </summary>
    public class CreatingDuplicateEntryException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CreatingDuplicateEntryException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CreatingDuplicateEntryException(string message)
            : base(message) { }

        /// <summary>
        /// Constructs a detailed error message indicating the presence of duplicate values for a specific property.
        /// </summary>
        /// <param name="propertyName">The name of the property that has duplicate values.</param>
        /// <param name="duplicateValues">The duplicate values encountered.</param>
        /// <returns>A detailed error message about the duplicate entries.</returns>
        public static string MessageFromDuplicateValues(string propertyName, IEnumerable<string> duplicateValues)
        {
            var sb = new StringBuilder(
                $"There's more than one element with the same value in the '{propertyName}' attribute.");
            sb.Append(" ");
            sb.Append(CommonException.MessageFromDuplicateValues(propertyName, duplicateValues));
            return sb.ToString();
        }
    }


    /// <summary>
    /// Exception for signaling that an operation failed because it attempted to create an entity that already exists.
    /// </summary>
    public class EntityAlreadyExistsException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EntityAlreadyExistsException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public EntityAlreadyExistsException(string message)
            : base(message) { }

        /// <summary>
        /// Constructs a detailed error message indicating that entities already exist, identifying them by a specific property.
        /// </summary>
        /// <param name="propertyName">The name of the property used to identify existing entities.</param>
        /// <param name="duplicateValues">The values that were found to be duplicates, causing the exception.</param>
        /// <returns>A detailed error message about the existing entities.</returns>
        public static string MessageFromDuplicateValues(string propertyName, IEnumerable<string> duplicateValues)
        {
            var sb = new StringBuilder(
                $"Error trying to create entities that already exists, looking at the '{propertyName}' attribute.");
            sb.Append(" ");
            sb.Append(CommonException.MessageFromDuplicateValues(propertyName, duplicateValues));

            return sb.ToString();
        }
    }

}
