namespace Dapper.Wrapper
{
    using FastCrud;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using SqlManager;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text.Json;
    using System.Threading.Tasks;

    /// <summary>
    /// The DapperWrapperSqlManager class extends the functionality of DapperWrapper, offering specialized methods 
    /// for Insert, Update, and Delete operations. It simplifies and encapsulates common database operations, providing 
    /// a higher-level abstraction over DapperWrapper. It manages transaction commit and rollback internally, 
    /// ensuring that database operations are automatically committed or rolled back based on the operation's success 
    /// or failure. Additionally, this class integrates logging via an ILogger instance, and it logs important messages or 
    /// errors related to the database operations it performs, enhancing debugging and monitoring capabilities.
    /// Designed for extensibility, all public and protected methods in DapperWrapperSqlManager are virtual, allowing 
    /// for easy customization and extension to tailor its functionality to specific requirements.
    /// </summary>
    /// <remarks>
    /// While DapperWrapper provides basic functionalities with manual control over transactions through Commit and 
    /// Rollback methods, DapperWrapperSqlManager abstracts these details, offering a streamlined and error-resistant 
    /// interface. It is particularly useful in scenarios where transaction management should be abstracted away from 
    /// the business logic, ensuring full completion or rollback of operations, thus maintaining database consistency 
    /// and integrity. The virtual design of its methods encourages inheritance and customization, providing flexibility 
    /// for developers to extend and adapt the class to various use cases.
    /// </remarks>
    public class DapperWrapperSqlManager : DapperWrapper
    {
        private readonly ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the DapperWrapperSqlManager class, setting up the necessary configuration, 
        /// logger, connection information, and SQL dialect for database operations.
        /// </summary>
        /// <param name="configuration">The configuration object containing settings such as connection strings or other parameters necessary for the database operations.</param>
        /// <param name="logger">The ILogger instance used for logging information, warnings, or errors encountered during the database operations.</param>
        /// <param name="nameOrConnectionString">The name of the connection string or the connection string itself, used to establish database connections.</param>
        /// <param name="sqlDialect">An enumeration value of type SqlDialect that specifies the SQL dialect to be used by Dapper.
        /// This ensures that SQL generated or interpreted by Dapper is appropriate for the specific database being targeted (e.g., SQL Server, MySQL, PostgreSQL).</param>
        /// <remarks>
        /// This constructor is responsible for initializing the DapperWrapperSqlManager with all the necessary components and settings for it to perform the intended database operations effectively.
        /// By encapsulating the logger within the manager, it allows for seamless and centralized logging throughout the database operation lifecycle.
        /// </remarks>
        public DapperWrapperSqlManager(IConfiguration configuration, ILogger logger,
            string nameOrConnectionString, SqlDialect sqlDialect)
            : base(configuration, nameOrConnectionString, sqlDialect)
        {
            _logger = logger;
        }

        /// <summary>
        /// Asynchronously inserts an entity into the database, providing options to manage the transaction and error handling.
        /// This method directly modifies the provided entity instance, reflecting any changes made during the insert operation
        /// (e.g., setting auto-generated primary key values).
        /// </summary>
        /// <param name="entity">The entity to be inserted into the database. The entity should match the structure of the corresponding database table.</param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a <see cref="DbOperationResult{T}"/>
        /// that contains the inserted entity and a flag indicating whether the insert operation was successful.
        /// The entity in the results will be the same instance as that in the input, potentially modified to reflect any changes made during the insertion
        /// (e.g., assignment of auto-generated primary keys).
        /// </returns>
        /// <remarks>
        /// This method inserts the provided entity in the database, allowing the caller to specify whether the method
        /// should handle transaction management and error handling. The method modifies the input entity instance directly,
        /// making it essential for callers to be aware that the passed entity will reflect the state after insertion.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<DbOperationResult<T>> InsertEntityAsync<T>(T entity, bool manageTransaction = true,
            bool throwOnError = true)
        {
            var methodDescription = GetMethodDescription<T>();
            return await this.ExecuteDbOperation(methodDescription, entity, this.InsertEntityCallback,
                manageTransaction, throwOnError);
        }

        /// <summary>
        /// Asynchronously updates an existing entity in the database, providing options to manage the transaction and error handling.
        /// </summary>
        /// <param name="entity">The entity to be updated in the database.
        /// The entity should match the structure of the corresponding database table
        /// and should include the primary key to identify the record to update.</param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a <see cref="DbOperationResult{T}"/> that contains the updated entity
        /// and a flag indicating whether the update operation was successful.
        /// </returns>
        /// <remarks>
        /// This method updates the provided entity in the database, allowing the caller to specify whether the method
        /// should handle transaction management and error handling.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<DbOperationResult<T>> UpdateEntityAsync<T>(T entity, bool manageTransaction = true,
            bool throwOnError = true)
        {
            var methodDescription = GetMethodDescription<T>();
            var result = await this.ExecuteDbOperation(methodDescription, entity, this.UpdateEntityCallback,
                manageTransaction, throwOnError);
            return result;
        }

        /// <summary>
        /// Asynchronously deletes an existing entity in the database, providing options to manage the transaction and error handling.
        /// </summary>
        /// <param name="entity">The entity to be deleted from the database.
        /// The entity should match the structure of the corresponding database table
        /// and should include the primary key to identify the record to delete.</param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a <see cref="DbOperationResult{T}"/> that contains the updated entity
        /// and a flag indicating whether the update operation was successful.
        /// </returns>
        /// <remarks>
        /// This method deletes the provided entity in the database, allowing the caller to specify whether the method
        /// should handle transaction management and error handling.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<DbOperationResult<T>> DeleteEntityAsync<T>(T entity, bool manageTransaction = true,
            bool throwOnError = true)
        {
            var methodDescription = GetMethodDescription<T>();
            var result = await this.ExecuteDbOperation(methodDescription, entity, this.DeleteEntityCallback,
                manageTransaction, throwOnError);
            return result;
        }

        /// <summary>
        /// Asynchronously inserts a list of entities into the database, providing options to manage the transaction and error handling.
        /// This method directly modifies the provided entities instances, reflecting any changes made during the insert operation
        /// (e.g., setting auto-generated primary key values).
        /// </summary>
        /// <param name="entities">The entity to be inserted into the database. The entity should match the structure of the corresponding database table.</param>
        /// <param name="shouldCheckForDuplicatedNames">A boolean value indicating whether the method should check for duplicated values in the 'Name' property
        /// among the provided entities and against existing entities in the database. If the entities type do not inherit from BaseEntityWithNameAndAutoGeneratedId,
        /// all checks will be skipped. 
        /// If set to true, the method will invoke the CheckForDuplicatedNames method to ensure name uniqueness before proceeding with the main operation.
        /// If a name duplication is detected, the method will throw an exception and halt further execution.
        /// If set to false, the method will skip this check and directly proceed with the intended operation, potentially allowing entities with duplicate names to be processed.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a list of <see cref="DbOperationResult{T}"/> objects,
        /// each indicating the outcome of the insert operation for the corresponding entity in the input list.
        /// The entities in the results will be the same instances as those in the input, potentially modified to reflect any changes made during the insertion
        /// (e.g., assignment of auto-generated primary keys).
        /// </returns>
        /// <remarks>
        /// This method inserts the provided entities in the database, allowing the caller to specify whether the method
        /// should handle transaction management and error handling. The method modifies the input entities instance directly,
        /// making it essential for callers to be aware that the passed entities will reflect the state after insertion.
        /// Each entity in the input list is processed individually, allowing for detailed results for each insert operation.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<List<DbOperationResult<T>>> InsertEntitiesAsync<T>(List<T> entities,
            bool shouldCheckForDuplicatedNames = true, bool manageTransaction = true, bool throwOnError = true)
        {
            var methodDescription = GetMethodDescription<T>();
            var result = await this.ExecuteDbOperations(methodDescription, entities, this.InsertEntityCallback,
                shouldCheckForDuplicatedNames, manageTransaction, throwOnError);
            return result;
        }

        /// <summary>
        /// Asynchronously updates an existing a list of entities in the database, providing options to manage the transaction and error handling.
        /// </summary>
        /// <param name="entities">The entities to be updated in the database.
        /// The entities should match the structure of the corresponding database table
        /// and should include the primary key to identify the record to update.</param>
        /// <param name="shouldCheckForDuplicatedNames">A boolean value indicating whether the method should check for duplicated values in the 'Name' property
        /// among the provided entities and against existing entities in the database. If the entities type do not inherit from BaseEntityWithNameAndAutoGeneratedId,
        /// all checks will be skipped. 
        /// If set to true, the method will invoke the CheckForDuplicatedNames method to ensure name uniqueness before proceeding with the main operation.
        /// If a name duplication is detected, the method will throw an exception and halt further execution.
        /// If set to false, the method will skip this check and directly proceed with the intended operation, potentially allowing entities with duplicate names to be processed.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a list of <see cref="DbOperationResult{T}"/> objects,
        /// each containing the original entity and a flag indicating whether the update operation was successful.
        /// </returns>
        /// <remarks>
        /// This method updates the provided entities in the database, allowing the caller to specify whether the method
        /// should handle transaction management and error handling.
        /// Each entity in the input list is processed individually, allowing for detailed results for each update operation.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<List<DbOperationResult<T>>> UpdateEntitiesAsync<T>(List<T> entities,
            bool shouldCheckForDuplicatedNames = true, bool manageTransaction = true, bool throwOnError = true)
        {
            var methodDescription = GetMethodDescription<T>();
            var result = await this.ExecuteDbOperations(methodDescription, entities, this.UpdateEntityCallback,
                shouldCheckForDuplicatedNames, manageTransaction, throwOnError);
            return result;
        }

        /// <summary>
        /// Asynchronously deletes an existing a list of entities in the database, providing options to manage the transaction and error handling.
        /// </summary>
        /// <param name="entities">The entities to be deleted in the database.
        /// The entities should match the structure of the corresponding database table
        /// and should include the primary key to identify the record to delete.</param>
        /// <param name="shouldCheckForDuplicatedNames">A boolean value indicating whether the method should check for duplicated values in the 'Name' property
        /// among the provided entities and against existing entities in the database. If the entities type do not inherit from BaseEntityWithNameAndAutoGeneratedId,
        /// all checks will be skipped. 
        /// If set to true, the method will invoke the CheckForDuplicatedNames method to ensure name uniqueness before proceeding with the main operation.
        /// If a name duplication is detected, the method will throw an exception and halt further execution.
        /// If set to false, the method will skip this check and directly proceed with the intended operation, potentially allowing entities with duplicate names to be processed.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a list of <see cref="DbOperationResult{T}"/> objects,
        /// each containing the original entity and a flag indicating whether the delete operation was successful.
        /// </returns>
        /// <remarks>
        /// This method deletes the provided entities in the database, allowing the caller to specify whether the method
        /// should handle transaction management and error handling.
        /// Each entity in the input list is processed individually, allowing for detailed results for each delete operation.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<List<DbOperationResult<T>>> DeleteEntitiesAsync<T>(List<T> entities,
            bool shouldCheckForDuplicatedNames = false, bool manageTransaction = true, bool throwOnError = true)
        {
            var methodDescription = GetMethodDescription<T>();
            var result = await this.ExecuteDbOperations(methodDescription, entities, this.DeleteEntityCallback,
                shouldCheckForDuplicatedNames, manageTransaction, throwOnError);
            return result;
        }

        /// <summary>
        /// Asynchronously performs a bulk deletion of entities from the database based on their IDs. This method is optimized for 
        /// handling multiple deletions in a single operation, improving performance over individual delete calls.
        /// </summary>
        /// <param name="entities">A read-only collection of entities to be deleted.
        /// Each entity should have its Id set, as it will be used to identify the record to delete.
        /// The entities should inherit from BaseEntityWithAutoGeneratedId, ensuring they have an Id property.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <returns>A task representing the asynchronous operation, resulting in a boolean value indicating whether the bulk delete operation was successful.
        /// The operation is considered successful if all entities are deleted; otherwise, false is returned.</returns>
        /// <remarks>
        /// This method provides an efficient way to delete multiple entities based on their IDs, which is particularly useful
        /// in scenarios where a large number of deletions need to be performed, and individual delete calls are not practical.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        public virtual async Task<bool> BulkDeleteEntitiesByIdAsync<T>(IReadOnlyCollection<T> entities,
            bool manageTransaction = true) where T : BaseEntityWithAutoGeneratedId
        {
            var entitiesIds = entities.Select(x => x.Id).ToList();
            return await this.BulkDeleteEntitiesByIdAsync<T>(entitiesIds, manageTransaction);
        }

        /// <summary>
        /// Asynchronously performs a bulk deletion of entities from the database based on their IDs. This method is optimized for 
        /// handling multiple deletions in a single operation, improving performance over individual delete calls.
        /// </summary>
        /// <param name="entitiesIds">A read-only collection of entity IDs to be deleted. The IDs should correspond to the primary key values of the entities in the database.
        /// The generic type T must be a type that inherits from BaseEntityWithAutoGeneratedId, which ensures that each entity has an identifiable Id.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <returns>A task representing the asynchronous operation, resulting in a boolean value indicating whether the bulk delete operation was successful.
        /// The operation is considered successful if all entities are deleted; otherwise, false is returned.</returns>
        /// <remarks>
        /// This method provides an efficient way to delete multiple entities by their IDs, which is particularly useful
        /// in scenarios where a large number of deletions need to be performed, and individual delete calls are not practical.
        /// It is designed to be overridden in derived classes for customized behavior as needed.
        /// </remarks>
        public virtual async Task<bool> BulkDeleteEntitiesByIdAsync<T>(IReadOnlyCollection<int> entitiesIds,
            bool manageTransaction = true) where T : BaseEntityWithAutoGeneratedId
        {
            var methodDescription = GetMethodDescription<T>();
            var entityName = typeof(T).Name;
            var numberOfEntitiesToDelete = entitiesIds.Count;
            var timer = Stopwatch.StartNew();
            var result = false;
            try
            {
                FormattableString filter =
                    $"{nameof(BaseEntityWithAutoGeneratedId.Id):C} {entitiesIds.ConvertToSqlIn()}";
                var deleted = await this.BulkDeleteAsync<T>(filter);
                if (deleted != entitiesIds.Count)
                {
                    throw new Exception(
                        $"Trying to delete {numberOfEntitiesToDelete} {entityName}, but instead only {deleted} were deleted");
                }

                if (manageTransaction)
                {
                    this.CommitChanges();
                }

                result = true;
            }
            catch (Exception ex)
            {
                this.HandleSqlError<T>(ex, methodDescription, manageTransaction);
                result = false;
            }
            finally
            {
                this.LogExecutedFunction(result, methodDescription, timer);
            }

            return result;
        }

        /// <summary>
        /// Executes a database transaction for a given operation on an entity. This method is designed to encapsulate common transaction logic, 
        /// providing a consistent approach to executing database operations with transaction management and error handling.
        /// </summary>
        /// <param name="methodDescription">A description of the database operation being performed, typically used for logging purposes to identify the operation context in logs.</param>
        /// <param name="entity">The entity on which the database operation is being performed.
        /// This entity is passed to the callback function to execute the specific database operation.
        /// </param>
        /// <param name="callback">A callback function that encapsulates the actual database operation (e.g., insert, update, delete) to be performed on the entity.
        /// This function is expected to be asynchronous and to handle the operation directly on the database.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a <see cref="DbOperationResult{T}"/>
        /// that contains the entity and a flag indicating whether the operation was successful.
        /// The entity in the result is the same instance as the input, potentially modified by the callback operation.</returns>
        /// <remarks>
        /// This method provides a generic, reusable way to perform and manage database operations,
        /// ensuring consistent behavior across different types of operations in terms of transaction management and error handling.
        /// The method is primarily intended for internal use by other methods within the class that perform database operations.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        protected virtual async Task<DbOperationResult<T>> ExecuteDbOperation<T>(string methodDescription,
            T entity, Func<T, Task> callback, bool manageTransaction, bool throwOnError)
        {
            var timer = Stopwatch.StartNew();
            var result = false;
            try
            {
                await callback(entity);
                if (manageTransaction)
                {
                    this.CommitChanges();
                }

                result = true;
            }
            catch (Exception ex)
            {
                this.HandleSqlError<T>(ex, methodDescription, manageTransaction);
                if (throwOnError)
                {
                    throw;
                }

                result = false;
            }
            finally
            {
                this.LogExecutedFunction(result, methodDescription, timer);
            }

            return new DbOperationResult<T>(entity, result);
        }

        /// <summary>
        /// Executes a database transaction for a given operation on a list of entities. This method is designed to encapsulate common transaction logic, 
        /// providing a consistent approach to executing database operations with transaction management and error handling.
        /// </summary>
        /// <param name="methodDescription">A description of the database operation being performed, typically used for logging purposes to identify the operation context in logs.</param>
        /// <param name="entities">The list of entities on which the database operation is being performed.
        /// These entities are passed to the callback function to execute the specific database operation.
        /// </param>
        /// <param name="callback">A callback function that encapsulates the actual database operation (e.g., insert, update, delete) to be performed on the entities.
        /// This function is expected to be asynchronous and to handle the operation directly on the database.
        /// </param>
        /// <param name="shouldCheckForDuplicatedNames">A boolean value indicating whether the method should check for duplicated values in the 'Name' property
        /// among the provided entities and against existing entities in the database. If the entities type do not inherit from BaseEntityWithNameAndAutoGeneratedId,
        /// all checks will be skipped. 
        /// If set to true, the method will invoke the CheckForDuplicatedNames method to ensure name uniqueness before proceeding with the main operation.
        /// If a name duplication is detected, the method will throw an exception and halt further execution.
        /// If set to false, the method will skip this check and directly proceed with the intended operation, potentially allowing entities with duplicate names to be processed.
        /// </param>
        /// <param name="manageTransaction">Determines whether the transaction is managed internally within the method.
        /// If set to true, the method will start a new transaction and commit it after the insert operation is successful, or roll it back if an error occurs.
        /// If set to false, the transaction management must be handled externally.
        /// </param>
        /// <param name="throwOnError">Specifies whether an exception should be thrown if the insert operation fails.
        /// If set to true, any error during the insert operation will result in an exception being thrown.
        /// If set to false, errors will be handled silently, and the operation result will indicate success or failure.
        /// </param>
        /// <returns>A task that represents the asynchronous operation, resulting in a list of <see cref="DbOperationResult{T}"/> objects,
        /// each containing the entity and a flag indicating whether the operation was successful.
        /// The entities in the result are the same instances as the input, potentially modified by the callback operation.</returns>
        /// <remarks>
        /// This method provides a generic, reusable way to perform and manage database operations,
        /// ensuring consistent behavior across different types of operations in terms of transaction management and error handling.
        /// The method is primarily intended for internal use by other methods within the class that perform database operations.
        /// It is designed in a way that allows for it to be overridden in derived classes for customized behavior, as needed.
        /// </remarks>
        protected virtual async Task<List<DbOperationResult<T>>> ExecuteDbOperations<T>(string methodDescription,
            List<T> entities, Func<T, Task> callback, bool shouldCheckForDuplicatedNames,
            bool manageTransaction, bool throwOnError)
        {
            if (!entities.Any())
            {
                return [];
            }

            if (shouldCheckForDuplicatedNames)
            {
                await this.CheckForDuplicatedNames(entities);
            }

            var dbOperationsResults = new List<DbOperationResult<T>>();
            var timer = Stopwatch.StartNew();
            try
            {
                foreach (var entity in entities)
                {
                    var operationResult = false;

                    try
                    {
                        await callback(entity);
                        operationResult = true;
                    }
                    catch (Exception ex)
                    {
                        operationResult = false;
                        var errorMessage =
                            $"Error on on {methodDescription} --> {JsonSerializer.Serialize(entity)}";
                        if (throwOnError)
                        {
                            this.HandleSqlError<T>(ex, methodDescription, manageTransaction);
                            throw new Exception(errorMessage, ex);
                        }

                        _logger.LogError(ex, errorMessage);
                    }
                    finally
                    {
                        dbOperationsResults.Add(new DbOperationResult<T>(entity, operationResult));
                    }
                }

                if (manageTransaction)
                {
                    this.CommitChanges();
                }
                return dbOperationsResults;
            }
            finally
            {
                var didFunctionSucceed = dbOperationsResults.All(x => x.Succeeded);
                this.LogExecutedFunction(didFunctionSucceed, methodDescription, timer);
            }
        }

        /// <summary>
        /// Checks a collection of entities for duplicate names within the collection itself and against existing entries in the database. 
        /// If duplicates are found, an exception is thrown to prevent the creation of entities with non-unique names.
        /// </summary>
        /// <param name="entities">A read-only collection of entities to be checked for duplicate names.
        /// The entities should inherit from BaseEntityWithNameAndAutoGeneratedId to ensure they have a 'Name' property.
        /// Otherwise, the method would just skip all checks.</param>
        /// <remarks>
        /// This method first checks for any duplicate names within the provided collection. If duplicates are found within the collection, 
        /// a CreatingDuplicateEntryException is thrown, indicating that there are entities with the same name in the input collection.
        /// Subsequently, it checks against the existing entities in the database. If any name in the collection matches an existing entity's name,
        /// an EntityAlreadyExistsException is thrown, indicating that an entity with the same name already exists in the database.
        /// This double-check ensures the uniqueness of the 'Name' field among entities, both in the collection and the database.
        /// The method is designed to be used as a safeguard in operations where unique names are mandatory, such as inserting or updating entities in bulk.
        /// </remarks>
        /// <exception cref="CreatingDuplicateEntryException">Thrown when two or more entities in the input collection have the same name.</exception>
        /// <exception cref="EntityAlreadyExistsException">Thrown when one or more entities in the input collection have names that already exist in the database.</exception>
        protected virtual async Task CheckForDuplicatedNames<T>(IReadOnlyCollection<T> entities)
        {
            if (!typeof(T).IsSubclassOf(typeof(BaseEntityWithNameAndAutoGeneratedId)))
            {
                return;
            }

            var entitiesAsBaseEntityWithName = entities
                .Cast<BaseEntityWithNameAndAutoGeneratedId>()
                .ToList();

            var duplicateNames = entitiesAsBaseEntityWithName
                .GroupBy(e => e.Name)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key!)
                .ToList();
            if (duplicateNames.Any())
            {
                var message = CreatingDuplicateEntryException.MessageFromDuplicateValues(
                    nameof(BaseEntityWithNameAndAutoGeneratedId.Name),
                    duplicateNames);
                throw new CreatingDuplicateEntryException(message);
            }

            var currentEntities = await this.FindAsync<T>();
            duplicateNames = currentEntities.Cast<BaseEntityWithNameAndAutoGeneratedId>()
                .Select(x => x.Name!)
                .Intersect(entitiesAsBaseEntityWithName.Select(x => x.Name!))
                .ToList();
            if (duplicateNames.Any())
            {
                var message = EntityAlreadyExistsException.MessageFromDuplicateValues(
                    nameof(BaseEntityWithNameAndAutoGeneratedId.Name),
                    duplicateNames);
                throw new EntityAlreadyExistsException(message);
            }
        }

        private static string GetMethodDescription<T>([CallerMemberName] string methodName = "")
        {
            return $"{methodName} - {typeof(T).Name}";
        }

        private void HandleSqlError<T>(Exception ex, string methodDescription, bool manageTransaction)
        {
            _logger.LogError(ex, $"PSQL '{methodDescription}' threw an exception. Entity Type: {typeof(T).Name}");
            if (manageTransaction)
            {
                this.RollbackChanges();
            }
        }

        private void LogExecutedFunction(bool didFunctionSucceed, string methodDescription, Stopwatch timer)
        {
            var resultValue = "NEGATIVE";
            if (didFunctionSucceed)
            {
                resultValue = "POSITIVE";
            }

            _logger.LogDebug("PSQL {Function} executed in {Milliseconds}ms with {Result} result.",
                methodDescription, timer.ElapsedMilliseconds, resultValue);
        }

        private async Task InsertEntityCallback<T>(T entity)
        {
            await this.InsertAsync(entity);
        }

        private async Task UpdateEntityCallback<T>(T entity)
        {
            var updated = await this.UpdateAsync(entity);
            if (!updated)
            {
                throw new Exception("Update result was false but didn't throw exception");
            }
        }

        private async Task DeleteEntityCallback<T>(T entity)
        {
            var deleted = await this.DeleteAsync(entity);
            if (!deleted)
            {
                throw new Exception("Delete result was false but didn't throw exception");
            }
        }
    }
}