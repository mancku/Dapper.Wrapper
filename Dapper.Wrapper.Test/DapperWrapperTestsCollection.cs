﻿namespace Dapper.Wrapper.Test
{
    [CollectionDefinition("DapperWrapperTestsCollection")]
    public class DapperWrapperTestsCollection : ICollectionFixture<TestContainersHandlerFixture>
    {
        // This class has no code and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}