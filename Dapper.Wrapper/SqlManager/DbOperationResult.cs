namespace Dapper.Wrapper.SqlManager
{
    public record DbOperationResult<T>(T Entity, bool Succeeded)
    {
        public T Entity { get; } = Entity;
        public bool Succeeded { get; } = Succeeded;
    }
}
