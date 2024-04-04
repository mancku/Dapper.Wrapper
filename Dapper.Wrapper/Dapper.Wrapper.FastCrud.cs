namespace Dapper.Wrapper
{
    using FastCrud;
    using FastCrud.Configuration.StatementOptions.Builders;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public partial class DapperWrapper
    {
        /// <summary>
        /// Queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="orderBy">A FormattableString specifying the ORDER BY clause to determine the order of the returned records. The words 'ORDER BY' should not be in it. If null, the order is unspecified.</param>
        /// <param name="isTransactional">A boolean value indicating whether the query should be executed within a transaction. If true, the query is executed as part of a transaction; otherwise, it is executed independently.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the query to execute. If null, the default command timeout is used.</param>
        /// <param name="top">An optional long value specifying the maximum number of records to return. If null, all matching records are returned.</param>
        /// <param name="skip">An optional long value indicating the number of records to skip before starting to return the records. This parameter is typically used for pagination. If null, no records are skipped.</param>
        /// <returns>An IEnumerable of type TEntity containing the records that match the query criteria.</returns>
        public IEnumerable<TEntity> Find<TEntity>(FormattableString? filter = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var options = this.SetRangedBatchSqlStatementOptions<TEntity>(isTransactional, filter, orderBy,
                commandTimeout, top, skip);
            return this.GetConnection(isTransactional).Find(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="options">An action that accepts an IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder instance of TEntity, 
        /// allowing the caller to configure the query options, such as specifying filtering, ordering, and paging parameters.</param>
        /// <returns>An IEnumerable of type TEntity containing the records that match the query criteria.</returns>
        public IEnumerable<TEntity> Find<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return this.GetConnection(standardOptions).Find(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="orderBy">A FormattableString specifying the ORDER BY clause to determine the order of the returned records. The words 'ORDER BY' should not be in it. If null, the order is unspecified.</param>
        /// <param name="isTransactional">A boolean value indicating whether the query should be executed within a transaction. If true, the query is executed as part of a transaction; otherwise, it is executed independently.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the query to execute. If null, the default command timeout is used.</param>
        /// <param name="top">An optional long value specifying the maximum number of records to return. If null, all matching records are returned.</param>
        /// <param name="skip">An optional long value indicating the number of records to skip before starting to return the records. This parameter is typically used for pagination. If null, no records are skipped.</param>
        /// <returns>A List of type TEntity containing the records that match the query criteria.</returns>
        public List<TEntity> FindAsList<TEntity>(FormattableString? filter = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            return this.Find<TEntity>(filter, orderBy, isTransactional, commandTimeout, top, skip)
                .ToList();
        }

        /// <summary>
        /// Queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="options">An action that accepts an IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder instance of TEntity, 
        /// allowing the caller to configure the query options, such as specifying filtering, ordering, and paging parameters.</param>
        /// <returns>A List of type TEntity containing the records that match the query criteria.</returns>
        public List<TEntity> FindAsList<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            return this.Find(options).ToList();
        }

        /// <summary>
        /// Asynchronously queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="orderBy">A FormattableString specifying the ORDER BY clause to determine the order of the returned records. The words 'ORDER BY' should not be in it. If null, the order is unspecified.</param>
        /// <param name="isTransactional">A boolean value indicating whether the query should be executed within a transaction. If true, the query is executed as part of a transaction; otherwise, it is executed independently.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the query to execute. If null, the default command timeout is used.</param>
        /// <param name="top">An optional long value specifying the maximum number of records to return. If null, all matching records are returned.</param>
        /// <param name="skip">An optional long value indicating the number of records to skip before starting to return the records. This parameter is typically used for pagination. If null, no records are skipped.</param>
        /// <returns>A Task of IEnumerable of type TEntity containing the records that match the query criteria.</returns>
        public async Task<IEnumerable<TEntity>> FindAsync<TEntity>(FormattableString? filter = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var options = this.SetRangedBatchSqlStatementOptions<TEntity>(isTransactional, filter,
                orderBy, commandTimeout, top, skip);
            return await this.GetConnection(isTransactional).FindAsync(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="options">An action that accepts an IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder instance of TEntity, 
        /// allowing the caller to configure the query options, such as specifying filtering, ordering, and paging parameters.</param>
        /// <returns>A Task of IEnumerable of type TEntity containing the records that match the query criteria.</returns>
        public async Task<IEnumerable<TEntity>> FindAsync<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return await this.GetConnection(standardOptions).FindAsync(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="orderBy">A FormattableString specifying the ORDER BY clause to determine the order of the returned records. The words 'ORDER BY' should not be in it. If null, the order is unspecified.</param>
        /// <param name="isTransactional">A boolean value indicating whether the query should be executed within a transaction. If true, the query is executed as part of a transaction; otherwise, it is executed independently.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the query to execute. If null, the default command timeout is used.</param>
        /// <param name="top">An optional long value specifying the maximum number of records to return. If null, all matching records are returned.</param>
        /// <param name="skip">An optional long value indicating the number of records to skip before starting to return the records. This parameter is typically used for pagination. If null, no records are skipped.</param>
        /// <returns>A Task of List of type TEntity containing the records that match the query criteria.</returns>
        public async Task<List<TEntity>> FindAsListAsync<TEntity>(FormattableString? filter = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var result = await this.FindAsync<TEntity>(filter, orderBy, isTransactional, commandTimeout, top, skip);
            return result.ToList();
        }

        /// <summary>
        /// Asynchronously queries the database for a set of records based on specified conditions and parameters, allowing customization of the query's behavior and results.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned records.</typeparam>
        /// <param name="options">An action that accepts an IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder instance of TEntity, 
        /// allowing the caller to configure the query options, such as specifying filtering, ordering, and paging parameters.</param>
        /// <returns>A Task of List of type TEntity containing the records that match the query criteria.</returns>
        public async Task<List<TEntity>> FindAsListAsync<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            var result = await this.FindAsync(options);
            return result.ToList();
        }

        /// <summary>
        /// Queries the database for a single record based on its Primary Key(s).
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned record.</typeparam>
        /// <param name="entityKeys">The entity with the needed value(s) set in the property/properties with the Key attribute</param>
        /// <param name="isTransactional">A boolean value indicating whether the query should be executed within a transaction. If true, the query is executed as part of a transaction; otherwise, it is executed independently.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the query to execute. If null, the default command timeout is used.</param>
        /// <returns>Returns a single entity or NULL if none could be found.</returns>
        public TEntity? Get<TEntity>(TEntity entityKeys, bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetSelectStatementOptions<TEntity>(isTransactional, commandTimeout);
            return this.Get(entityKeys, options);
        }

        /// <summary>
        /// Queries the database for a single record based on its Primary Key(s).
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned record.</typeparam>
        /// <param name="entityKeys">The entity with the needed value(s) set in the property/properties with the Key attribute</param>
        /// <param name="options">An action that accepts an ISelectSqlStatementOptionsBuilder instance of TEntity, 
        /// allowing the caller to configure the timeout and whether the query should be executed within a transaction.</param>
        /// <returns>Returns a single entity or NULL if none could be found.</returns>
        public TEntity? Get<TEntity>(TEntity entityKeys, Action<ISelectSqlStatementOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return this.GetConnection(standardOptions).Get(entityKeys, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously queries the database for a single record based on its Primary Key(s).
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned record.</typeparam>
        /// <param name="entityKeys">The entity with the needed value(s) set in the property/properties with the Key attribute</param>
        /// <param name="isTransactional">A boolean value indicating whether the query should be executed within a transaction. If true, the query is executed as part of a transaction; otherwise, it is executed independently.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the query to execute. If null, the default command timeout is used.</param>
        /// <returns>Returns a Task of a single entity or NULL if none could be found.</returns>
        public async Task<TEntity?> GetAsync<TEntity>(TEntity entityKeys, bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetSelectStatementOptions<TEntity>(isTransactional, commandTimeout);
            return await this.GetAsync(entityKeys, options);
        }

        /// <summary>
        /// Asynchronously queries the database for a single record based on its Primary Key(s).
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity being queried. This type determines the structure of the returned record.</typeparam>
        /// <param name="entityKeys">The entity with the needed value(s) set in the property/properties with the Key attribute</param>
        /// <param name="options">An action that accepts an ISelectSqlStatementOptionsBuilder instance of TEntity, 
        /// allowing the caller to configure the timeout and whether the query should be executed within a transaction.</param>
        /// <returns>Returns a Task of a single entity or NULL if none could be found.</returns>
        public async Task<TEntity?> GetAsync<TEntity>(TEntity entityKeys, Action<ISelectSqlStatementOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return await this.GetConnection(standardOptions).GetAsync(entityKeys, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Counts all the records in a table or a range of records if a filter is specified.
        /// </summary>
        /// <typeparam name="TEntity">The entity type for which the count is being calculated. This determines the table or collection that the query targets.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="isTransactional">A boolean value indicating whether the count operation should be executed within a transaction. If true, the operation is part of a transaction; otherwise, it is executed independently. Defaults to false.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the count operation to complete. If null, the default command timeout is used.</param>
        /// <returns>An integer representing the count of entities that match the specified conditions.</returns>
        public int Count<TEntity>(FormattableString? filter = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalSqlStatementOptions<TEntity>(isTransactional, filter, commandTimeout);
            return this.GetConnection(isTransactional).Count(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously counts all the records in a table or a range of records if a filter is specified.
        /// </summary>
        /// <typeparam name="TEntity">The entity type for which the count is being calculated. This determines the table or collection that the query targets.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="isTransactional">A boolean value indicating whether the count operation should be executed within a transaction. If true, the operation is part of a transaction; otherwise, it is executed independently. Defaults to false.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the count operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Task of an integer representing the count of entities that match the specified conditions.</returns>
        public async Task<int> CountAsync<TEntity>(FormattableString? filter = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalSqlStatementOptions<TEntity>(isTransactional, filter, commandTimeout);
            return await this.GetConnection(isTransactional).CountAsync(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Inserts a new record into the database based on the entity provided.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be inserted. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToInsert">The entity object that should be inserted into the database. This object's properties should map to the fields or columns of the corresponding table or collection in the database.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <remarks>This method does not return a value. If the operation is successful, the objectToInsert Entity would have its properties updated based on the database generated fields.</remarks>
        public void Insert<TEntity>(TEntity objectToInsert, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            this.GetConnection(isTransactional).Insert(objectToInsert, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously inserts a new record into the database based on the entity provided.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be inserted. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToInsert">The entity object that should be inserted into the database. This object's properties should map to the fields or columns of the corresponding table or collection in the database.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <remarks>This method does not return a value. If the operation is successful, the objectToInsert Entity would have its properties updated based on the database generated fields.</remarks>
        public async Task InsertAsync<TEntity>(TEntity objectToInsert, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            await this.GetConnection(isTransactional).InsertAsync(objectToInsert, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Updates a record in the database based on the entity provided.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be updated. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToUpdate">The entity object that should be updated into the database. This object's properties should map to the fields or columns of the corresponding table or collection in the database.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Boolean indicating whether the operation has been successful.</returns>
        public bool Update<TEntity>(TEntity objectToUpdate, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return this.GetConnection(isTransactional).Update(objectToUpdate, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously updates a record in the database based on the entity provided.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be updated. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToUpdate">The entity object that should be updated into the database. This object's properties should map to the fields or columns of the corresponding table or collection in the database.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Task of Boolean indicating whether the operation has been successful.</returns>
        public async Task<bool> UpdateAsync<TEntity>(TEntity objectToUpdate, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return await this.GetConnection(isTransactional).UpdateAsync(objectToUpdate, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Updates all the records in the table or a range of records if a filter was set.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be updated. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToUpdate">
        /// The data used to update the records. 
        /// The primary keys will be ignored.
        /// For partial updates use an entity mapping override.
        /// </param>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>The number of records updated.</returns>
        public int BulkUpdate<TEntity>(TEntity objectToUpdate, FormattableString? filter, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<TEntity>(isTransactional, filter, commandTimeout);
            return this.GetConnection(isTransactional).BulkUpdate(objectToUpdate, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously updates all the records in the table or a range of records if a filter was set.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be updated. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToUpdate">
        /// The data used to update the records. 
        /// The primary keys will be ignored.
        /// For partial updates use an entity mapping override.
        /// </param>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Task of int with the number of records updated.</returns>
        public async Task<int> BulkUpdateAsync<TEntity>(TEntity objectToUpdate, FormattableString? filter,
            bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<TEntity>(isTransactional, filter, commandTimeout);
            return await this.GetConnection(isTransactional).BulkUpdateAsync(objectToUpdate, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Deletes an entity by its primary keys.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be deleted. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToDelete">The entity you wish to remove.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Task of int with the number of records updated.</returns>
        /// <returns>A Boolean indicating whether the operation has been successful.</returns>
        public bool Delete<TEntity>(TEntity objectToDelete, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return this.GetConnection(isTransactional).Delete(objectToDelete, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously deletes an entity by its primary keys.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be deleted. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="objectToDelete">The entity you wish to remove.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Task of int with the number of records updated.</returns>
        /// <returns>A Task of Boolean indicating whether the operation has been successful.</returns>
        public async Task<bool> DeleteAsync<TEntity>(TEntity objectToDelete, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return await this.GetConnection(isTransactional).DeleteAsync(objectToDelete, Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Deletes all the records in the table or a range of records if a filter was set.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be deleted. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>The number of records deleted.</returns>
        public int BulkDelete<TEntity>(FormattableString? filter, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<TEntity>(isTransactional, filter, commandTimeout);
            return this.GetConnection(isTransactional).BulkDelete(Transaction, this.OverrideDialectInOptions(options));
        }

        /// <summary>
        /// Asynchronously deletes all the records in the table or a range of records if a filter was set.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity to be deleted. This type determines the table or collection where the new record will be added.</typeparam>
        /// <param name="filter">A FormattableString representing the WHERE clause conditions to filter the query results. The word 'WHERE' should not be in it. If null, no filtering is applied.</param>
        /// <param name="isTransactional">A boolean value indicating whether the operation should be executed within a transaction. If true, the operation is executed as part of a transaction, providing atomicity and ensuring data integrity. Defaults to true.</param>
        /// <param name="commandTimeout">An optional TimeSpan specifying the maximum amount of time to wait for the operation to complete. If null, the default command timeout is used.</param>
        /// <returns>A Task of int with the number of records deleted.</returns>
        public async Task<int> BulkDeleteAsync<TEntity>(FormattableString? filter, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<TEntity>(isTransactional, filter, commandTimeout);
            return await this.GetConnection(isTransactional).BulkDeleteAsync(Transaction, this.OverrideDialectInOptions(options));
        }

        public void OverrideJoinDialect<TEntity>(ISqlJoinOptionsBuilder<TEntity> join)
        {
            var dialectOverride = this.GetDialectOverride<TEntity>();
            join.WithEntityMappingOverride(dialectOverride);
        }

        private Action<IConditionalBulkSqlStatementOptionsBuilder<TEntity>> SetConditionalBulkStatementOptions<TEntity>(
          bool useCurrentTransaction = false, FormattableString? filter = null,
          TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                if (!string.IsNullOrEmpty(filter?.ToString() ?? string.Empty))
                {
                    query.Where(filter);
                }

                query.WithTimeout(commandTimeout);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<IStandardSqlStatementOptionsBuilder<TEntity>> SetStandardStatementOptions<TEntity>(
            bool useCurrentTransaction = false, TimeSpan? commandTimeout = null)
        {
            return query =>
             {
                 query.ShouldUseTransaction(useCurrentTransaction);
                 query.WithTimeout(commandTimeout);
                 this.OverrideStatementDialect(query);
             };
        }

        private Action<ISelectSqlStatementOptionsBuilder<TEntity>> SetSelectStatementOptions<TEntity>(
            bool useCurrentTransaction = false, TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.WithTimeout(commandTimeout);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> SetRangedBatchSqlStatementOptions<TEntity>(
            bool useCurrentTransaction = false, FormattableString? filter = null, FormattableString? orderBy = null,
            TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.Where(filter);
                query.OrderBy(orderBy);
                query.WithTimeout(commandTimeout);
                query.Top(top);
                query.Skip(skip);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<IConditionalSqlStatementOptionsBuilder<TEntity>> SetConditionalSqlStatementOptions<TEntity>(
            bool useCurrentTransaction = false, FormattableString? filter = null,
            TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.Where(filter);
                query.WithTimeout(commandTimeout);
                this.OverrideStatementDialect(query);
            };
        }

        private void OverrideStatementDialect<TEntity>(IConditionalBulkSqlStatementOptionsBuilder<TEntity> query)
        {
            var dialectOverride = this.GetDialectOverride<TEntity>();
            query.WithEntityMappingOverride(dialectOverride);
        }

        private void OverrideStatementDialect<TEntity>(IStandardSqlStatementOptionsBuilder<TEntity> query)
        {
            var dialectOverride = this.GetDialectOverride<TEntity>();
            query.WithEntityMappingOverride(dialectOverride);
        }

        private void OverrideStatementDialect<TEntity>(ISelectSqlStatementOptionsBuilder<TEntity> query)
        {
            var dialectOverride = this.GetDialectOverride<TEntity>();
            query.WithEntityMappingOverride(dialectOverride);
        }

        private void OverrideStatementDialect<TEntity>(IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity> query)
        {
            var dialectOverride = this.GetDialectOverride<TEntity>();
            query.WithEntityMappingOverride(dialectOverride);
        }

        private void OverrideStatementDialect<TEntity>(IConditionalSqlStatementOptionsBuilder<TEntity> query)
        {
            var dialectOverride = this.GetDialectOverride<TEntity>();
            query.WithEntityMappingOverride(dialectOverride);
        }

        private Action<IConditionalBulkSqlStatementOptionsBuilder<TEntity>> OverrideDialectInOptions<TEntity>(
            Action<IConditionalBulkSqlStatementOptionsBuilder<TEntity>> options)
        {
            return query =>
            {
                options(query);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<IStandardSqlStatementOptionsBuilder<TEntity>> OverrideDialectInOptions<TEntity>(
            Action<IStandardSqlStatementOptionsBuilder<TEntity>> options)
        {
            return query =>
            {
                options(query);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<ISelectSqlStatementOptionsBuilder<TEntity>> OverrideDialectInOptions<TEntity>(
            Action<ISelectSqlStatementOptionsBuilder<TEntity>> options)
        {
            return query =>
            {
                options(query);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> OverrideDialectInOptions<TEntity>(
            Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            return query =>
            {
                options(query);
                this.OverrideStatementDialect(query);
            };
        }

        private Action<IConditionalSqlStatementOptionsBuilder<TEntity>> OverrideDialectInOptions<TEntity>(
            Action<IConditionalSqlStatementOptionsBuilder<TEntity>> options)
        {
            return query =>
            {
                options(query);
                this.OverrideStatementDialect(query);
            };
        }

        /// <summary>
        /// Enforcing the dialect specific for this instance of DapperWrapper
        /// Because the Dapper.FastCrud is not well prepared for multi-db
        /// https://github.com/MoonStorm/FastCrud/discussions/160
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        private FastCrud.Mappings.EntityMapping<TEntity> GetDialectOverride<TEntity>()
        {
            var dialectOverride = OrmConfiguration.GetDefaultEntityMapping<TEntity>()
                .Clone().SetDialect(_sqlDialect);
            return dialectOverride;
        }
    }
}
