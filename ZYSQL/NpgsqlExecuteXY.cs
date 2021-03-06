﻿//by luyikk 2010.5.9

using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
namespace ZYSQL
{
    public class NpgSqlExecuteXY : ListDeserializerBase, IDisposable
    {
        private readonly object _lockThis = new object();


        #region 静态

        /// <summary>
        ///     CONN连接对象池
        /// </summary>
        public readonly Dictionary<string, ObjectPool<NpgsqlConnection>> DbConnPool;

        public readonly ObjectPool<NpgsqlCommand> DbCommandPool;


        #endregion



        #region 事务处理

        public byte TransStats { get; set; }

        /// <summary>
        ///     开始一个事务
        /// </summary>
        public void BeginTrans()
        {
            if (DbConn.State == ConnectionState.Closed)
                DbConn.Open();

            Trans = DbConn.BeginTransaction();
            Command.Transaction = Trans;

            TransStats = 1;
        }

        /// <summary>
        ///     提交一个事务
        /// </summary>
        public void CommitTrans()
        {
            Trans.Commit();
            TransStats = 3;
        }

        /// <summary>
        ///     终止回滚一个事务
        /// </summary>
        public void RollbackTrans()
        {
            Trans.Rollback();
            TransStats = 2;
        }

        #endregion

        public NpgSqlExecuteXY()
        {
            DbConnPool = SqlInstance.Instance.GetNpgsqlConnectionPool();
            DbCommandPool = SqlInstance.Instance.GetNpgsqlCommandPool();


            DbConn = DbConnPool["DefautConnection"].GetObject();
            if (DbConn == null)
                throw new Exception("Sql Connection obj is NULL,Please Look LogOut ERROR Msg!!");

            Key = "DefautConnection";
            Command = DbCommandPool.GetObject();
            Command.Connection = DbConn;
        }


        public NpgSqlExecuteXY(string key)
        {
            DbConnPool = SqlInstance.Instance.GetNpgsqlConnectionPool();
            DbCommandPool = SqlInstance.Instance.GetNpgsqlCommandPool();

            DbConn = DbConnPool[key].GetObject();

            Key = key;

            if (DbConn == null)
                throw new Exception(
                    string.Format("Sql Connection obj is NULL,Please Look LogOut ERROR Msg!! For KEY:{0}", key));

            Command = DbCommandPool.GetObject();
            Command.Connection = DbConn;
        }

        /// <summary>
        ///     数据库连接器
        /// </summary>
        public NpgsqlConnection DbConn { get; protected set; }

        /// <summary>
        ///     命令
        /// </summary>
        public NpgsqlCommand Command { get; protected set; }

        private NpgsqlTransaction Trans { get; set; }

        public string Key { get; }

        /// <summary>
        ///     释放
        /// </summary>
        public void Dispose()
        {
            Dispose(false);
        }


        /// <summary>
        ///     打开数据库连接
        /// </summary>
        public void Open()
        {
            if (DbConn.State != ConnectionState.Open)
                DbConn.Open();
        }

        /// <summary>
        ///     关闭数据库连接
        /// </summary>
        public void Close()
        {
            if (DbConn.State == ConnectionState.Open)
                DbConn.Close();
        }

        public void Dispose(bool isDispose)
        {
            if (TransStats == 1)
                RollbackTrans();

            TransStats = 0;

            if (isDispose)
            {
                DbConn.Close();
                DbConn.Dispose();
                Command.Dispose();
                DbConn = null;
                Command = null;
            }
            else
            {
                if (DbConn != null)
                    DbConnPool[Key].ReleaseObject(DbConn);
                if (Command != null)
                    DbCommandPool.ReleaseObject(Command);

                DbConn = null;
                Command = null;
            }
        }


        #region ExecuteNonQuery
        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>行数</returns>
        public int SqlExecuteNonQuery(string sql)
        {
            return SqlExecuteNonQuery(sql, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public int SqlExecuteNonQuery(string sql, bool bolIsProcedure)
        {
            return SqlExecuteNonQuery(sql, bolIsProcedure, null);
        }


        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public int SqlExecuteNonQuery(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExecuteNonQuery(sql, false, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns>行数</returns>
        public int SqlExecuteNonQuery(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                return Command.ExecuteNonQuery();
            }
        }

        #endregion

        #region ExecuteNonQueryAsync
        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>行数</returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql)
        {
            return SqlExecuteNonQueryAsync(sql, null);
        }

        /// <summary>
        /// 运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="cannToken">CancellationToken</param>
        /// <returns>行数</returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, CancellationToken cannToken)
        {
            return SqlExecuteNonQueryAsync(sql, cannToken, null);
        }



        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, bool bolIsProcedure)
        {
            return SqlExecuteNonQueryAsync(sql, bolIsProcedure, CancellationToken.None, null);
        }

        /// <summary>
        /// 运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <param name="cannToken">CancellationToken</param>
        /// <returns></returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, bool bolIsProcedure, CancellationToken token)
        {
            return SqlExecuteNonQueryAsync(sql, bolIsProcedure, token, null);
        }


        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExecuteNonQueryAsync(sql, false, CancellationToken.None, parem);
        }


        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="cannToken">CancellationToken</param>
        /// <returns></returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, CancellationToken canntoken, params NpgsqlParameter[] parem)
        {
            return SqlExecuteNonQueryAsync(sql, false, canntoken, parem);
        }


        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="cannToken">CancellationToken</param>
        /// <returns></returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, bool bolIsProcedure = false, params NpgsqlParameter[] parem)
        {
            return SqlExecuteNonQueryAsync(sql, bolIsProcedure, CancellationToken.None, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns>行数</returns>
        public Task<int> SqlExecuteNonQueryAsync(string sql, bool bolIsProcedure, CancellationToken cannToken, params NpgsqlParameter[] parem)
        {

            Command.CommandText = sql;
            Command.Parameters.Clear();
            if (parem != null)
                Command.Parameters.AddRange(parem);
            Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
            return Command.ExecuteNonQueryAsync(cannToken);

        }

        #endregion


        #region ExecuteReader
        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql,
            CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            return SqlExecuteReader(sql, commandBehavior, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql, bool bolIsProcedure,
            CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            return SqlExecuteReader(sql, bolIsProcedure, commandBehavior, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql, CommandBehavior commandBehavior,
            params NpgsqlParameter[] parem)
        {
            return SqlExecuteReader(sql, false, commandBehavior, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql, bool bolIsProcedure, CommandBehavior commandBehavior,
            params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                return Command.ExecuteReader(commandBehavior);
            }
        }

        #endregion

        #region ExecuteReaderAsync
        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql,
            CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            return SqlExecuteReaderAsync(sql, commandBehavior, null);
        }

        /// <summary>
        /// 运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, CancellationToken canToken)
        {
            return SqlExecuteReaderAsync(sql, CommandBehavior.Default, canToken, null);
        }

        /// <summary>
        /// 运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, CancellationToken canToken, params NpgsqlParameter[] parem)
        {
            return SqlExecuteReaderAsync(sql, CommandBehavior.Default, canToken, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, bool bolIsProcedure,
            CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            return SqlExecuteReaderAsync(sql, bolIsProcedure, commandBehavior, CancellationToken.None, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, bool bolIsProcedure,
            CommandBehavior commandBehavior = CommandBehavior.Default, params NpgsqlParameter[] parem)
        {
            return SqlExecuteReaderAsync(sql, bolIsProcedure, commandBehavior, CancellationToken.None, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, bool bolIsProcedure, CancellationToken canToken = default)
        {
            return SqlExecuteReaderAsync(sql, bolIsProcedure, CommandBehavior.Default, canToken, null);
        }


        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, CommandBehavior commandBehavior,
            params NpgsqlParameter[] parem)
        {
            return SqlExecuteReaderAsync(sql, false, commandBehavior, CancellationToken.None, parem);
        }


        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public Task<DbDataReader> SqlExecuteReaderAsync(string sql, CommandBehavior commandBehavior, CancellationToken canToken = default,
            params NpgsqlParameter[] parem)
        {
            return SqlExecuteReaderAsync(sql, false, commandBehavior, canToken, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public async Task<DbDataReader> SqlExecuteReaderAsync(string sql, bool bolIsProcedure, CommandBehavior commandBehavior, CancellationToken canToken,
            params NpgsqlParameter[] parem)
        {

            Command.CommandText = sql;
            Command.Parameters.Clear();
            if (parem != null)
                Command.Parameters.AddRange(parem);
            Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
            return await Command.ExecuteReaderAsync(commandBehavior, canToken);

        }

        #endregion


        #region ExecuteScalar
        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql)
        {
            return SqlExecuteScalar(sql, false, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, bool bolIsProcedure)
        {
            return SqlExecuteScalar(sql, bolIsProcedure, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExecuteScalar(sql, false, parem);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                return Command.ExecuteScalar();
            }
        }

        #endregion

        #region ExecuteScalarAsync
        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql)
        {
            return SqlExecuteScalarAsync(sql, false, CancellationToken.None, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql, CancellationToken canToken)
        {
            return SqlExecuteScalarAsync(sql, false, canToken, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql, bool bolIsProcedure)
        {
            return SqlExecuteScalarAsync(sql, bolIsProcedure, CancellationToken.None, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql, bool bolIsProcedure, CancellationToken canToken)
        {
            return SqlExecuteScalarAsync(sql, bolIsProcedure, canToken, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExecuteScalarAsync(sql, false, CancellationToken.None, parem);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql, CancellationToken canToken, params NpgsqlParameter[] parem)
        {
            return SqlExecuteScalarAsync(sql, false, canToken, parem);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public Task<object> SqlExecuteScalarAsync(string sql, bool bolIsProcedure, CancellationToken canToken, params NpgsqlParameter[] parem)
        {

            Command.CommandText = sql;
            Command.Parameters.Clear();
            if (parem != null)
                Command.Parameters.AddRange(parem);
            Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
            return Command.ExecuteScalarAsync(canToken);

        }

        #endregion


        #region ExcuteDataSet
        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql)
        {
            return SqlExcuteDataSet(sql, null);
        }

        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExcuteDataSet(sql, false, parem);
        }

        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            return SqlExcuteDataSet(sql, "this", bolIsProcedure, parem);
        }


        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="tablename">DATASET TABLE name</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, string tablename, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;

                var adapter = new NpgsqlDataAdapter(Command);

                var dataset = new DataSet();

                adapter.Fill(dataset, tablename);

                adapter.Dispose();

                return dataset;
            }
        }

        #endregion




        #region ExcuteSelectObject
        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql) where T : class, new()
        {
            return SqlExcuteSelectObject<T>(sql, false);
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure) where T : class, new()
        {
            return SqlExcuteSelectObject<T>(sql, bolIsProcedure, null);
        }




        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, params NpgsqlParameter[] parem) where T : class, new()
        {
            return SqlExcuteSelectObject<T>(sql, false, parem);
        }




        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
            where T : class, new()
        {
            return SqlExcuteSelectObject(sql, bolIsProcedure, out T _, parem);
        }



        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj, params NpgsqlParameter[] parem) where T : class, new()
        {
            return SqlExcuteSelectObject(sql, false, out obj, parem);
        }

        /// <summary>
        ///     返回第一个结果
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="first">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns>结果数量</returns>
        public T SqlExcuteSelectFirst<T>(string sql, params NpgsqlParameter[] parem) where T : class, new()
        {

            var i = SqlExcuteSelectObject(sql, false, out T first, parem).Count;

            if (i > 0)
            {
                return first;
            }
            else
                return null;
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj) where T : class, new()
        {
            return SqlExcuteSelectObject(sql, false, out obj, null);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="tablename"></param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <param name="obj">填充对象</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, out T obj,
            params NpgsqlParameter[] parem) where T : class, new()
        {
            lock (_lockThis)
            {


                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                var objList = Deserializer<T>(Command);
                obj = objList.FirstOrDefault<T>();
                return objList;
            }
        }


        #endregion

        #region ExcuteSelectObjectAsync
        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <returns>对象集合</returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql) where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, false, CancellationToken.None, null);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, CancellationToken cantoken) where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, false, cantoken, null);
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <param name="bolIsProcedure"></param>
        /// <returns></returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, bool bolIsProcedure) where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, bolIsProcedure, CancellationToken.None, null);
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <returns>对象集合</returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, bool bolIsProcedure, CancellationToken cantoken) where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, bolIsProcedure, cantoken, null);
        }




        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns>对象集合</returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, params NpgsqlParameter[] parem) where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, false, CancellationToken.None, parem);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <param name="parem"></param>
        /// <returns></returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, CancellationToken canToken, params NpgsqlParameter[] parem)
            where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, false, canToken, parem);
        }




        /// <summary>
        ///     返回第一个结果
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="first">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns>结果数量</returns>
        public async Task<T> SqlExcuteSelectFirstAsync<T>(string sql, params NpgsqlParameter[] parem) where T : class, new()
        {
            return (await SqlExcuteSelectObjectAsync<T>(sql, false, CancellationToken.None, parem)).FirstOrDefault<T>();
        }

        /// <summary>
        ///     返回第一个结果
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="first">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns>结果数量</returns>
        public async Task<T> SqlExcuteSelectFirstAsync<T>(string sql, CancellationToken token, params NpgsqlParameter[] parem) where T : class, new()
        {
            return (await SqlExcuteSelectObjectAsync<T>(sql, false, token, parem)).FirstOrDefault<T>();
        }



        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="tableName">表名</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <returns>对象集合</returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem) where T : class, new()
        {
            return SqlExcuteSelectObjectAsync<T>(sql, bolIsProcedure, CancellationToken.None, parem);
        }



        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="tablename"></param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <param name="obj">填充对象</param>
        /// <returns>对象集合</returns>
        public Task<List<T>> SqlExcuteSelectObjectAsync<T>(string sql, bool bolIsProcedure, CancellationToken canToken, params NpgsqlParameter[] parem) where T : class, new()
        {

            Command.CommandText = sql;
            Command.Parameters.Clear();
            if (parem != null)
                Command.Parameters.AddRange(parem);
            Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
            var objList = DeserializerAsync<T>(Command, canToken);
            return objList;

        }


        #endregion


        #region SExcuteUpdateOrInsertOrDeleteObject
        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public int SqlExcuteUpdateOrInsertOrDeleteObject<T>(string sql, T obj)
        {
            return SqlExcuteUpdateOrInsertOrDeleteObject(sql, false, obj);
        }

        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public int SqlExcuteUpdateOrInsertOrDeleteObject<T>(string sql, bool bolIsProcedure, T obj)
        {
            lock (_lockThis)
            {
                var objType = obj.GetType();

                var propertyArray = TypeOfCacheManager.GetInstance().GetTypeProperty(objType).Values;

                Command.CommandText = sql;
                Command.Parameters.Clear();

                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;

                foreach (var props in propertyArray)
                {
                    var values = props.GetValue(obj, null);

                    if (values != null)
                        Command.Parameters.AddWithValue("@" + props.Name, values);
                }

                return Command.ExecuteNonQuery();
            }
        }

        #endregion

        #region SExcuteUpdateOrInsertOrDeleteObjectAsync
        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public Task<int> SqlExcuteUpdateOrInsertOrDeleteObjectAsync<T>(string sql, T obj)
        {
            return SqlExcuteUpdateOrInsertOrDeleteObjectAsync(sql, false, CancellationToken.None, obj);
        }

        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public Task<int> SqlExcuteUpdateOrInsertOrDeleteObjectAsync<T>(string sql, CancellationToken canToken, T obj)
        {
            return SqlExcuteUpdateOrInsertOrDeleteObjectAsync(sql, false, canToken, obj);
        }

        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public Task<int> SqlExcuteUpdateOrInsertOrDeleteObjectAsync<T>(string sql, bool bolIsProcedure, CancellationToken canToken, T obj)
        {

            var objType = obj.GetType();

            var propertyArray = TypeOfCacheManager.GetInstance().GetTypeProperty(objType).Values;

            Command.CommandText = sql;
            Command.Parameters.Clear();

            Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;

            foreach (var props in propertyArray)
            {
                var values = props.GetValue(obj, null);

                if (values != null)
                    Command.Parameters.AddWithValue("@" + props.Name, values);
            }

            return Command.ExecuteNonQueryAsync(canToken);

        }

        #endregion
    }
}