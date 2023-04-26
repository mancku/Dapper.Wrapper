namespace Dapper.Wrapper
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Dapper.FastCrud;
    using System.Threading.Tasks;
    using Dapper.FastCrud.Configuration.StatementOptions.Builders;

    public partial class DapperWrapper
    {
        public IEnumerable<T> Find<T>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var options = this.SetRangedBatchSqlStatementOptions<T>(isTransactional, whereConditions, orderBy,
                commandTimeout, top, skip);
            return this.GetConnection(isTransactional).Find(this.Transaction, options);
        }

        public List<T> FindAsList<T>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            return this.Find<T>(whereConditions, orderBy, isTransactional, commandTimeout, top, skip)
                .ToList();
        }

        public IEnumerable<T> Find<T>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<T>> options,
            bool isTransactional = false)
        {
            return this.GetConnection(isTransactional).Find(this.Transaction, options);
        }

        public List<T> FindAsList<T>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<T>> options,
            bool isTransactional = false)
        {
            return this.Find(options, isTransactional).ToList();
        }

        public async Task<List<T>> FindAsListAsync<T>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var result = await this.FindAsync<T>(whereConditions, orderBy, isTransactional, commandTimeout, top, skip);
            return result.ToList();
        }

        public async Task<IEnumerable<T>> FindAsync<T>(FormattableString? whereConditions = null, FormattableString? orderBy = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null, long? top = null, long? skip = null)
        {
            var options = this.SetRangedBatchSqlStatementOptions<T>(isTransactional, whereConditions,
                orderBy, commandTimeout, top, skip);
            return await this.GetConnection(isTransactional).FindAsync(this.Transaction, options);
        }

        public async Task<List<T>> FindAsListAsync<T>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<T>> options)
        {
            var result = await this.FindAsync(options);
            return result.ToList();
        }

        public async Task<IEnumerable<T>> FindAsync<T>(Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<T>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<T>>;
            return await this.GetConnection(standardOptions).FindAsync(this.Transaction, options);
        }

        public T? Get<T>(T entityKeys, bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetSelectStatementOptions<T>(isTransactional, commandTimeout);
            return this.Get(entityKeys, options);
        }

        public T? Get<T>(T entityKeys, Action<ISelectSqlStatementOptionsBuilder<T>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<T>>;
            return this.GetConnection(standardOptions).Get(entityKeys, this.Transaction, options);
        }

        public async Task<T?> GetAsync<T>(T entityKeys, bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetSelectStatementOptions<T>(isTransactional, commandTimeout);
            return await this.GetAsync(entityKeys, options);
        }

        public async Task<T?> GetAsync<T>(T entityKeys, Action<ISelectSqlStatementOptionsBuilder<T>> options)
        {
            var standardOptions = options as Action<IStandardSqlStatementOptionsBuilder<T>>;
            return await this.GetConnection(standardOptions).GetAsync(entityKeys, this.Transaction, options);
        }

        public int Count<T>(FormattableString? whereConditions = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalSqlStatementOptions<T>(isTransactional, whereConditions, commandTimeout);
            return this.GetConnection(isTransactional).Count(this.Transaction, options);
        }

        public async Task<int> CountAsync<T>(FormattableString? whereConditions = null,
            bool isTransactional = false, TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalSqlStatementOptions<T>(isTransactional, whereConditions, commandTimeout);
            return await this.GetConnection(isTransactional).CountAsync(this.Transaction, options);
        }

        public void Insert<T>(T objectToInsert, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<T>(isTransactional, commandTimeout);
            this.GetConnection(isTransactional).Insert(objectToInsert, this.Transaction, options);
        }

        public async Task InsertAsync<T>(T objectToInsert, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<T>(isTransactional, commandTimeout);
            await this.GetConnection(isTransactional).InsertAsync(objectToInsert, this.Transaction, options);
        }

        public bool Update<T>(T objectToUpdate, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<T>(isTransactional, commandTimeout);
            return this.GetConnection(isTransactional).Update(objectToUpdate, this.Transaction, options);
        }

        public async Task<bool> UpdateAsync<T>(T objectToUpdate, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<T>(isTransactional, commandTimeout);
            return await this.GetConnection(isTransactional).UpdateAsync(objectToUpdate, this.Transaction, options);
        }

        public int BulkUpdate<T>(T objectToUpdate, FormattableString? whereConditions, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<T>(isTransactional, whereConditions, commandTimeout);
            return this.GetConnection(isTransactional).BulkUpdate(objectToUpdate, this.Transaction, options);
        }

        public async Task<int> BulkUpdateAsync<T>(T objectToUpdate, FormattableString? whereConditions,
            bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<T>(isTransactional, whereConditions, commandTimeout);
            return await this.GetConnection(isTransactional).BulkUpdateAsync(objectToUpdate, this.Transaction, options);
        }

        public bool Delete<T>(T objectToDelete, bool isTransactional = true, TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<T>(isTransactional, commandTimeout);
            return this.GetConnection(isTransactional).Delete(objectToDelete, this.Transaction, options);
        }

        public async Task<bool> DeleteAsync<T>(T objectToDelete, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetStandardStatementOptions<T>(isTransactional, commandTimeout);
            return await this.GetConnection(isTransactional).DeleteAsync(objectToDelete, this.Transaction, options);
        }

        public int BulkDelete<T>(FormattableString? whereConditions, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<T>(isTransactional, whereConditions, commandTimeout);
            return this.GetConnection(isTransactional).BulkDelete(this.Transaction, options);
        }

        public async Task<int> BulkDeleteAsync<T>(FormattableString? whereConditions, bool isTransactional = true,
            TimeSpan? commandTimeout = null)
        {
            var options = this.SetConditionalBulkStatementOptions<T>(isTransactional, whereConditions, commandTimeout);
            return await this.GetConnection(isTransactional).BulkDeleteAsync(this.Transaction, options);
        }

        private Action<IConditionalBulkSqlStatementOptionsBuilder<T>> SetConditionalBulkStatementOptions<T>(
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

        private Action<IStandardSqlStatementOptionsBuilder<T>> SetStandardStatementOptions<T>(
            bool useCurrentTransaction = false, TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.WithTimeout(commandTimeout);
            };
        }

        private Action<ISelectSqlStatementOptionsBuilder<T>> SetSelectStatementOptions<T>(
            bool useCurrentTransaction = false, TimeSpan? commandTimeout = null)
        {
            return query =>
            {
                query.ShouldUseTransaction(useCurrentTransaction);
                query.WithTimeout(commandTimeout);
            };
        }

        private Action<IRangedBatchSelectSqlSqlStatementOptionsOptionsBuilder<T>> SetRangedBatchSqlStatementOptions<T>(
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

        private Action<IConditionalSqlStatementOptionsBuilder<T>> SetConditionalSqlStatementOptions<T>(
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
