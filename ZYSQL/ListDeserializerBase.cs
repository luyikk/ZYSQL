using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading;
using System.Data.SQLite;

namespace ZYSQL
{
    public abstract class ListDeserializerBase
    {
        protected List<T> Deserializer<T>(IDbCommand command) where T : new()
        {
            using (var dataRead = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
            {
                  var func= DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);
                  return func(dataRead);
            }
        }

        protected async Task<List<T>> DeserializerAsync<T>(MySql.Data.MySqlClient.MySqlCommand command,CancellationToken canToken) where T : new()
        {
            using (var dataRead = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, canToken))
            {
                var func = DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);
                return func(dataRead);
            }
        }

        protected async Task<List<T>> DeserializerAsync<T>(System.Data.SqlClient.SqlCommand command, CancellationToken canToken) where T : new()
        {
            using (var dataRead = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, canToken))
            {
                var func = DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);
                return func(dataRead);
            }
        }

        protected async Task<List<T>> DeserializerAsync<T>(System.Data.Odbc.OdbcCommand command, CancellationToken canToken) where T : new()
        {
            using (var dataRead = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, canToken))
            {
                var func = DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);
                return func(dataRead);
            }
        }

        protected async Task<List<T>> DeserializerAsync<T>(Npgsql.NpgsqlCommand command, CancellationToken canToken) where T : new()
        {
            using (var dataRead = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, canToken))
            {
                var func = DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);
                return func(dataRead);
            }
        }

        protected async Task<List<T>> DeserializerAsync<T>(SQLiteCommand command, CancellationToken canToken) where T : new()
        {
            using (var dataRead = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, canToken))
            {
                var func = DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);
                return func(dataRead);
            }
        }
    }
}