namespace Dapper.Wrapper.SqlManager
{
    /// <summary>
    /// Represents the result of a database operation, encapsulating the entity involved and the outcome of the operation.
    /// </summary>
    /// <typeparam name="T">The type of the entity that the operation was performed on.</typeparam>
    public record DbOperationResult<T>(T Entity, bool Succeeded)
    {
        /// <summary>
        /// Gets the entity that was the subject of the database operation.
        /// </summary>
        public T Entity { get; } = Entity;

        /// <summary>
        /// Gets a value indicating whether the database operation succeeded.
        /// </summary>
        public bool Succeeded { get; } = Succeeded;
    }

}
