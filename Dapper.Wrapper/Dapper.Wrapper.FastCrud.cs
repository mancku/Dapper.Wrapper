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
        public IEnumerable<TEntity> Find<TEntity>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var options = SetRangedBatchSqlStatementOptions<TEntity>(isTransactional, whereConditions, orderBy,
                commandTimeout, top, skip);
            return this.GetConnection(isTransactional).Find(Transaction, options);
        }

        public List<TEntity> FindAsList<TEntity>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            return this.Find<TEntity>(whereConditions, orderBy, isTransactional, commandTimeout, top, skip)
                .ToList();
        }

        public IEnumerable<TEntity> Find<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options,
            bool isTransactional = false)
        {
            return this.GetConnection(isTransactional).Find(Transaction, options);
        }

        public List<TEntity> FindAsList<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options,
            bool isTransactional = false)
        {
            return this.Find(options, isTransactional).ToList();
        }

        public async Task<List<TEntity>> FindAsListAsync<TEntity>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var result = await this.FindAsync<TEntity>(whereConditions, orderBy, isTransactional, commandTimeout, top, skip);
            return result.ToList();
        }

        public async Task<IEnumerable<TEntity>> FindAsync<TEntity>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var options = SetRangedBatchSqlStatementOptions<TEntity>(isTransactional, whereConditions,
                orderBy, commandTimeout, top, skip);
            return await this.GetConnection(isTransactional).FindAsync(Transaction, options);
        }

        public async Task<List<TEntity>> FindAsListAsync<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            var result = await this.FindAsync(options);
            return result.ToList();
        }

        public async Task<IEnumerable<TEntity>> FindAsync<TEntity>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return await this.GetConnection(standardOptions).FindAsync(Transaction, options);
        }

        public TEntity? Get<TEntity>(TEntity entityKeys, bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = SetSelectStatementOptions<TEntity>(isTransactional, commandTimeout);
            return this.Get(entityKeys, options);
        }

        public TEntity? Get<TEntity>(TEntity entityKeys, Action<ISelectSqlStatementOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return this.GetConnection(standardOptions).Get(entityKeys, Transaction, options);
        }

        public async Task<TEntity?> GetAsync<TEntity>(TEntity entityKeys, bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = SetSelectStatementOptions<TEntity>(isTransactional, commandTimeout);
            return await this.GetAsync(entityKeys, options);
        }

        public async Task<TEntity?> GetAsync<TEntity>(TEntity entityKeys, Action<ISelectSqlStatementOptionsBuilder<TEntity>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<TEntity>>;
            return await this.GetConnection(standardOptions).GetAsync(entityKeys, Transaction, options);
        }

        public int Count<TEntity>(FormattableString? whereConditions = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = SetConditionalSqlStatementOptions<TEntity>(isTransactional, whereConditions, commandTimeout);
            return this.GetConnection(isTransactional).Count(Transaction, options);
        }

        public async Task<int> CountAsync<TEntity>(FormattableString? whereConditions = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = SetConditionalSqlStatementOptions<TEntity>(isTransactional, whereConditions, commandTimeout);
            return await this.GetConnection(isTransactional).CountAsync(Transaction, options);
        }

        public void Insert<TEntity>(TEntity objectToInsert, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            this.GetConnection(isTransactional).Insert(objectToInsert, Transaction, options);
        }

        public async Task InsertAsync<TEntity>(TEntity objectToInsert, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            await this.GetConnection(isTransactional).InsertAsync(objectToInsert, Transaction, options);
        }

        public bool Update<TEntity>(TEntity objectToUpdate, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return this.GetConnection(isTransactional).Update(objectToUpdate, Transaction, options);
        }

        public async Task<bool> UpdateAsync<TEntity>(TEntity objectToUpdate, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return await this.GetConnection(isTransactional).UpdateAsync(objectToUpdate, Transaction, options);
        }

        public int BulkUpdate<TEntity>(TEntity objectToUpdate, FormattableString? whereConditions, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = SetConditionalBulkStatementOptions<TEntity>(isTransactional, whereConditions, commandTimeout);
            return this.GetConnection(isTransactional).BulkUpdate(objectToUpdate, Transaction, options);
        }

        public async Task<int> BulkUpdateAsync<TEntity>(TEntity objectToUpdate, FormattableString? whereConditions,
            bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = SetConditionalBulkStatementOptions<TEntity>(isTransactional, whereConditions, commandTimeout);
            return await this.GetConnection(isTransactional).BulkUpdateAsync(objectToUpdate, Transaction, options);
        }

        public bool Delete<TEntity>(TEntity objectToDelete, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return this.GetConnection(isTransactional).Delete(objectToDelete, Transaction, options);
        }

        public async Task<bool> DeleteAsync<TEntity>(TEntity objectToDelete, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = SetStandardStatementOptions<TEntity>(isTransactional, commandTimeout);
            return await this.GetConnection(isTransactional).DeleteAsync(objectToDelete, Transaction, options);
        }

        public int BulkDelete<TEntity>(FormattableString? whereConditions, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = SetConditionalBulkStatementOptions<TEntity>(isTransactional, whereConditions, commandTimeout);
            return this.GetConnection(isTransactional).BulkDelete(Transaction, options);
        }

        public async Task<int> BulkDeleteAsync<TEntity>(FormattableString? whereConditions, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = SetConditionalBulkStatementOptions<TEntity>(isTransactional, whereConditions, commandTimeout);
            return await this.GetConnection(isTransactional).BulkDeleteAsync(Transaction, options);
        }

        private static Action<IConditionalBulkSqlStatementOptionsBuilder<TEntity>> SetConditionalBulkStatementOptions<TEntity>(
          bool useCurrentTransaction = false, FormattableString? whereConditions = null,
          TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                if (!string.IsNullOrEmpty(whereConditions?.ToString() ?? string.Empty))
                {
                    query.Where(whereConditions);
                }

                query.WithTimeout(commandTimeout);
            };
        }

        private static Action<IStandardSqlStatementOptionsBuilder<TEntity>> SetStandardStatementOptions<TEntity>(
            bool useCurrentTransaction = false, TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.WithTimeout(commandTimeout);
            };
        }

        private static Action<ISelectSqlStatementOptionsBuilder<TEntity>> SetSelectStatementOptions<TEntity>(
            bool useCurrentTransaction = false, TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.WithTimeout(commandTimeout);
            };
        }

        private static Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<TEntity>> SetRangedBatchSqlStatementOptions<TEntity>(
            bool useCurrentTransaction = false, FormattableString? whereConditions = null, FormattableString? orderBy = null,
            TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.Where(whereConditions);
                query.OrderBy(orderBy);
                query.WithTimeout(commandTimeout);
                query.Top(top);
                query.Skip(skip);
            };
        }

        private static Action<IConditionalSqlStatementOptionsBuilder<TEntity>> SetConditionalSqlStatementOptions<TEntity>(
            bool useCurrentTransaction = false, FormattableString? whereConditions = null,
            TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.Where(whereConditions);
                query.WithTimeout(commandTimeout);
            };
        }
    }
}
