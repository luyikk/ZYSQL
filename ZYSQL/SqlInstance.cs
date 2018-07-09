using MySql.Data.MySqlClient;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;

namespace ZYSQL
{
    public delegate void LogOutHandle(string log, object sender);
    public delegate string DeCodeConnStringHandle(string connstr);

    public class SqlInstance
    {

        #region staticobj
        private static readonly SqlInstance _instance;

        private readonly static object lockobj = new object();
        public static SqlInstance Instance => _instance;

        static SqlInstance()
        {
            lock (lockobj)
            {
                if(_instance==null)
                    _instance = new SqlInstance();
            }
        }

        #endregion


        #region 日记输出

        /// <summary>
        /// 日记错误输出事件
        /// </summary>
        public static event LogOutHandle LogOut;


        public static event DeCodeConnStringHandle DeCodeConnStr;


        private static string DeCodeConn(string connStr)
        {
            if (DeCodeConnStr != null)
                return DeCodeConnStr(connStr);
            else
                return connStr;
        }

        /// <summary>
        /// 输出日记
        /// </summary>
        /// <param name="log"></param>
        /// <param name="sender"></param>
        private static void LogOutMananger(string log, object sender)
        {
            if (LogOut != null)
            {
                LogOut(log, sender);
            }
            else
            {
                throw new Exception(log);
            }

        }

        #endregion

        private bool IsInstall { get; set; }

        private Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>> MySqlConnPool;
        private ObjectPool<MySql.Data.MySqlClient.MySqlCommand> MySqlCommandPool;
        private Dictionary<string, ObjectPool<System.Data.SqlClient.SqlConnection>> SqlConnPool;
        private ObjectPool<System.Data.SqlClient.SqlCommand> SqlCommandPool;
        private Dictionary<string, ObjectPool<System.Data.Odbc.OdbcConnection>> OdbcConnPool;
        private ObjectPool<System.Data.Odbc.OdbcCommand> OdbcCommandPool;
        private Dictionary<string, ObjectPool<Npgsql.NpgsqlConnection>> NpgsqlConnPool;
        private ObjectPool<Npgsql.NpgsqlCommand> NpgsqlCommandPool;


        private SqlInstance()
        {

        }
              


        public void InstallConfig(IEnumerable<DataConnectConfig> configuration)
        {
            lock(lockobj)
            {
                if (!IsInstall)
                {
                    MySqlInstance(configuration, (connpool, cmdpool) =>
                     {
                         MySqlConnPool = connpool;
                         MySqlCommandPool = cmdpool;
                     });

                    SqlServerInstance(configuration, (connpool, cmdpool) =>
                    {
                        SqlConnPool = connpool;
                        SqlCommandPool = cmdpool;
                    });

                    OdbcServerInstance(configuration, (connpool, cmdpool) =>
                    {
                        OdbcConnPool = connpool;
                        OdbcCommandPool = cmdpool;
                    });

                    NpgsqlServerInstance(configuration, (connpool, cmdpool) =>
                    {
                        NpgsqlConnPool = connpool;
                        NpgsqlCommandPool = cmdpool;
                    });

                    IsInstall = true;
                }
                else
                {
                    throw new Exception("重复加载配置文件");
                }
            }
        }


        public void InstallConfig()
        {
            lock (lockobj)
            {
                if (!IsInstall)
                {
                    var ConnPool1 = GetSqlConnInstance();
                    if (ConnPool1.Count > 0)
                        GetSqlCommandInstance();

                    var ConnPool2 = GetMySqlConnInstance();
                    if (ConnPool2.Count > 0)
                        GetMySqlCommandInstance();

                    var ConnPool3 = GetOdbcConnInstance();
                    if (ConnPool3.Count > 0)
                        GetOdbcCommandInstance();

                    var ConnPool4 = GetNpgsqlConnInstance();
                    if (ConnPool4.Count > 0)
                        GetNpgsqlCommandInstance();

                    IsInstall = true;
                }
                else
                {                    
                    throw new Exception("重复加载配置文件");
                }

            }
        }




        #region MySQL


        public Dictionary<string, ObjectPool<MySqlConnection>> GetMySqlConnectionPool()
        {
            return MySqlConnPool;
        }

        public ObjectPool<MySqlCommand> GetMySqlCommandPool()
        {
            return MySqlCommandPool;
        }

        #region configObj
        public  void MySqlInstance(IEnumerable<DataConnectConfig> configuration, Action<Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>>, ObjectPool<MySql.Data.MySqlClient.MySqlCommand>> action)
        {
            var ConnPool = GetMySqlConnInstance(configuration, out int cmdPoolCount);           
            var ConnCmd = GetMySqlCommandInstance(cmdPoolCount);
            action(ConnPool, ConnCmd);
        }


        /// <summary>
        /// 返回MYSQLCConn
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>> GetMySqlConnInstance(IEnumerable<DataConnectConfig> configuration, out int cmdPoolCount)
        {

            cmdPoolCount = -1;

            try
            {
                if (MySqlConnPool == null)
                {
                    MySqlConnPool = new Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>>();

                    foreach (var ar in configuration)
                    {
                        if (ar.SqlType.Equals("MYSQL", StringComparison.OrdinalIgnoreCase))
                        {
                            string name = ar.Name;
                            string connectionstring = ar.ConnectionString;
                            if (ar.MaxCount == 0)
                                ar.MaxCount = 1;


                            if (ar.IsEncode)
                                connectionstring = DeCodeConn(connectionstring);


                            ObjectPool<MySql.Data.MySqlClient.MySqlConnection> temp = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>(ar.MaxCount);

                            cmdPoolCount += ar.MaxCount;

                            temp.TheConstructor = typeof(MySql.Data.MySqlClient.MySqlConnection).GetConstructor(new Type[] { typeof(string) });

                            temp.Param = new object[] { connectionstring };

                            temp.GetObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                try
                                {
                                    conn.Open();
                                    return conn;
                                }
                                catch (Exception e)
                                {
                                    LogOutMananger(e.Message, e);
                                    return null;
                                }
                            });

                            temp.ReleaseObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                conn.Close();

                                return conn;
                            });


                            MySqlConnPool.Add(name, temp);
                        }
                    }


                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, MySqlConnPool);
            }



            return MySqlConnPool;
        }


        /// <summary>
        /// 返回MYSQLCommand对象
        /// </summary>
        /// <returns></returns>
        private ObjectPool<MySql.Data.MySqlClient.MySqlCommand> GetMySqlCommandInstance(int maxCount)
        {


            if (maxCount <=0)
                return null;
            try
            {
                if (MySqlCommandPool == null)
                {
                    MySqlCommandPool = new ObjectPool<MySql.Data.MySqlClient.MySqlCommand>(maxCount);
                    MySqlCommandPool.ReleaseObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlCommand>.ObjectRunTimeHandle((command, pool) =>
                    {
                        command.CommandText = "";
                        command.Connection = null;
                        command.CommandType = CommandType.Text;
                        command.Parameters.Clear();
                        return command;
                    });

                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, MySqlConnPool);
            }



            return MySqlCommandPool;
        }

        #endregion


        #region ConfigApp.Config


        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>> GetMySqlConnInstance()
        {

            if (MySqlConnPool == null)
            {
                MySqlConnPool = new Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>>();

                foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                {
                    if (ar.ProviderName.Equals("MySql.Data.MySqlClient", StringComparison.CurrentCulture))
                    {
                        string name = ar.Name;
                        string connectionstring = ar.ConnectionString;

                        if (name.IndexOf(":") > 0)
                        {
                            string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                            if (sp.Length == 2)
                            {
                                name = sp[0];

                                if (sp[1].ToUpper() == "ENCODE")
                                {
                                    connectionstring = DeCodeConn(connectionstring);
                                }
                            }

                        }

                        ObjectPool<MySql.Data.MySqlClient.MySqlConnection> temp = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                        temp.TheConstructor = typeof(MySql.Data.MySqlClient.MySqlConnection).GetConstructor(new Type[] { typeof(string) });

                        temp.Param = new object[] { connectionstring };

                        temp.GetObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            try
                            {
                                conn.Open();
                                return conn;
                            }
                            catch (Exception e)
                            {
                                LogOutMananger(e.Message, e);
                                return null;
                            }
                        });

                        temp.ReleaseObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            conn.Close();

                            return conn;
                        });


                        MySqlConnPool.Add(name, temp);
                    }
                }
            }


            return MySqlConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public ObjectPool<MySql.Data.MySqlClient.MySqlCommand> GetMySqlCommandInstance()
        {


            if (MySqlCommandPool == null)
            {
                MySqlCommandPool = new ObjectPool<MySql.Data.MySqlClient.MySqlCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                MySqlCommandPool.ReleaseObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlCommand>.ObjectRunTimeHandle((command, pool) =>
                {
                    command.CommandText = "";
                    command.Connection = null;
                    command.CommandType = CommandType.Text;
                    command.Parameters.Clear();
                    return command;
                });

            }
            
            return MySqlCommandPool;
        }


        #endregion
        #endregion

        #region SQLServer


        public Dictionary<string, ObjectPool<SqlConnection>> GetSqlConnectionPool()
        {
            return SqlConnPool;
        }

        public ObjectPool<SqlCommand> GetSqlCommandPool()
        {
            return SqlCommandPool;
        }

        #region configObj
        public void SqlServerInstance(IEnumerable<DataConnectConfig> configuration, Action<Dictionary<string, ObjectPool<SqlConnection>>, ObjectPool<SqlCommand>> action)
        {
            var ConnPool = GetSqlConnInstance(configuration, out int cmdPoolCount);
            var ConnCmd = GetSqlCommandInstance(cmdPoolCount);
            action(ConnPool, ConnCmd);
        }


        /// <summary>
        /// 返回SQLCConn
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, ObjectPool<SqlConnection>> GetSqlConnInstance(IEnumerable<DataConnectConfig> configuration, out int cmdPoolCount)
        {

            cmdPoolCount = -1;

            try
            {
                if (SqlConnPool == null)
                {
                    SqlConnPool = new Dictionary<string, ObjectPool<SqlConnection>>();

                    foreach (var ar in configuration)
                    {
                        if (ar.SqlType.Equals("SQLSERVER", StringComparison.OrdinalIgnoreCase))
                        {
                            string name = ar.Name;
                            string connectionstring = ar.ConnectionString;
                            if (ar.MaxCount == 0)
                                ar.MaxCount = 1;


                            if (ar.IsEncode)
                                connectionstring = DeCodeConn(connectionstring);


                            ObjectPool<SqlConnection> temp = new ObjectPool<SqlConnection>(ar.MaxCount);

                            cmdPoolCount += ar.MaxCount;

                            temp.TheConstructor = typeof(SqlConnection).GetConstructor(new Type[] { typeof(string) });

                            temp.Param = new object[] { connectionstring };

                            temp.GetObjectRunTime = new ObjectPool<SqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                try
                                {
                                    conn.Open();
                                    return conn;
                                }
                                catch (Exception e)
                                {
                                    LogOutMananger(e.Message, e);
                                    return null;
                                }
                            });

                            temp.ReleaseObjectRunTime = new ObjectPool<SqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                conn.Close();

                                return conn;
                            });


                            SqlConnPool.Add(name, temp);
                        }
                    }


                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, SqlConnPool);
            }



            return SqlConnPool;
        }


        /// <summary>
        /// 返回SQLCommand对象
        /// </summary>
        /// <returns></returns>
        private ObjectPool<SqlCommand> GetSqlCommandInstance(int maxCount)
        {


            if (maxCount <= 0)
                return null;
            try
            {
                if (SqlCommandPool == null)
                {
                    SqlCommandPool = new ObjectPool<SqlCommand>(maxCount);
                    SqlCommandPool.ReleaseObjectRunTime = new ObjectPool<SqlCommand>.ObjectRunTimeHandle((command, pool) =>
                    {
                        command.CommandText = "";
                        command.Connection = null;
                        command.CommandType = CommandType.Text;
                        command.Parameters.Clear();
                        return command;
                    });

                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, SqlConnPool);
            }



            return SqlCommandPool;
        }

        #endregion


        #region ConfigApp.Config



        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, ObjectPool<SqlConnection>> GetSqlConnInstance()
        {

            if (SqlConnPool == null)
            {
                SqlConnPool = new Dictionary<string, ObjectPool<SqlConnection>>();

                foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                {
                    if (ar.ProviderName.Equals("System.Data.SqlClient", StringComparison.CurrentCulture))
                    {
                        string name = ar.Name;
                        string connectionstring = ar.ConnectionString;

                        if (name.IndexOf(":") > 0)
                        {
                            string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                            if (sp.Length == 2)
                            {
                                name = sp[0];

                                if (sp[1].ToUpper() == "ENCODE")
                                {
                                    connectionstring = DeCodeConn(connectionstring);
                                }
                            }

                        }

                        ObjectPool<SqlConnection> temp = new ObjectPool<SqlConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                        temp.TheConstructor = typeof(SqlConnection).GetConstructor(new Type[] { typeof(string) });

                        temp.Param = new object[] { connectionstring };

                        temp.GetObjectRunTime = new ObjectPool<SqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            try
                            {
                                conn.Open();
                                return conn;
                            }
                            catch (Exception e)
                            {
                                LogOutMananger(e.Message, e);
                                return null;
                            }
                        });

                        temp.ReleaseObjectRunTime = new ObjectPool<SqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            conn.Close();

                            return conn;
                        });


                        SqlConnPool.Add(name, temp);
                    }
                }
            }


            return SqlConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public ObjectPool<SqlCommand> GetSqlCommandInstance()
        {


            if (SqlCommandPool == null)
            {
                SqlCommandPool = new ObjectPool<SqlCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                SqlCommandPool.ReleaseObjectRunTime = new ObjectPool<SqlCommand>.ObjectRunTimeHandle((command, pool) =>
                {
                    command.CommandText = "";
                    command.Connection = null;
                    command.CommandType = CommandType.Text;
                    command.Parameters.Clear();
                    return command;
                });

            }

            return SqlCommandPool;
        }


        #endregion
        #endregion

        #region ODBC


        public Dictionary<string, ObjectPool<OdbcConnection>> GetOdbcConnectionPool()
        {
            return OdbcConnPool;
        }

        public ObjectPool<OdbcCommand> GetOdbcCommandPool()
        {
            return OdbcCommandPool;
        }

        #region configObj
        public void OdbcServerInstance(IEnumerable<DataConnectConfig> configuration, Action<Dictionary<string, ObjectPool<OdbcConnection>>, ObjectPool<OdbcCommand>> action)
        {
            var ConnPool = GetOdbcConnInstance(configuration, out int cmdPoolCount);
            var ConnCmd = GetOdbcCommandInstance(cmdPoolCount);
            action(ConnPool, ConnCmd);
        }


        /// <summary>
        /// 返回OdbcCConn
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, ObjectPool<OdbcConnection>> GetOdbcConnInstance(IEnumerable<DataConnectConfig> configuration, out int cmdPoolCount)
        {

            cmdPoolCount = -1;

            try
            {
                if (OdbcConnPool == null)
                {
                    OdbcConnPool = new Dictionary<string, ObjectPool<OdbcConnection>>();

                    foreach (var ar in configuration)
                    {
                        if (ar.SqlType.Equals("ODBC", StringComparison.OrdinalIgnoreCase))
                        {
                            string name = ar.Name;
                            string connectionstring = ar.ConnectionString;
                            if (ar.MaxCount == 0)
                                ar.MaxCount = 1;


                            if (ar.IsEncode)
                                connectionstring = DeCodeConn(connectionstring);


                            ObjectPool<OdbcConnection> temp = new ObjectPool<OdbcConnection>(ar.MaxCount);

                            cmdPoolCount += ar.MaxCount;

                            temp.TheConstructor = typeof(OdbcConnection).GetConstructor(new Type[] { typeof(string) });

                            temp.Param = new object[] { connectionstring };

                            temp.GetObjectRunTime = new ObjectPool<OdbcConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                try
                                {
                                    conn.Open();
                                    return conn;
                                }
                                catch (Exception e)
                                {
                                    LogOutMananger(e.Message, e);
                                    return null;
                                }
                            });

                            temp.ReleaseObjectRunTime = new ObjectPool<OdbcConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                conn.Close();

                                return conn;
                            });


                            OdbcConnPool.Add(name, temp);
                        }
                    }


                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, OdbcConnPool);
            }



            return OdbcConnPool;
        }


        /// <summary>
        /// 返回OdbcCommand对象
        /// </summary>
        /// <returns></returns>
        private ObjectPool<OdbcCommand> GetOdbcCommandInstance(int maxCount)
        {


            if (maxCount <= 0)
                return null;
            try
            {
                if (OdbcCommandPool == null)
                {
                    OdbcCommandPool = new ObjectPool<OdbcCommand>(maxCount);
                    OdbcCommandPool.ReleaseObjectRunTime = new ObjectPool<OdbcCommand>.ObjectRunTimeHandle((command, pool) =>
                    {
                        command.CommandText = "";
                        command.Connection = null;
                        command.CommandType = CommandType.Text;
                        command.Parameters.Clear();
                        return command;
                    });

                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, OdbcConnPool);
            }



            return OdbcCommandPool;
        }

        #endregion


        #region ConfigApp.Config



        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, ObjectPool<OdbcConnection>> GetOdbcConnInstance()
        {

            if (OdbcConnPool == null)
            {
                OdbcConnPool = new Dictionary<string, ObjectPool<OdbcConnection>>();

                foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                {
                    
                    if (ar.ProviderName.Equals("System.Data.Odbc", StringComparison.CurrentCulture))
                    {
                        string name = ar.Name;
                        string connectionstring = ar.ConnectionString;

                        if (name.IndexOf(":") > 0)
                        {
                            string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                            if (sp.Length == 2)
                            {
                                name = sp[0];

                                if (sp[1].ToUpper() == "ENCODE")
                                {
                                    connectionstring = DeCodeConn(connectionstring);
                                }
                            }

                        }

                        ObjectPool<OdbcConnection> temp = new ObjectPool<OdbcConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                        temp.TheConstructor = typeof(OdbcConnection).GetConstructor(new Type[] { typeof(string) });

                        temp.Param = new object[] { connectionstring };

                        temp.GetObjectRunTime = new ObjectPool<OdbcConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            try
                            {
                                conn.Open();
                                return conn;
                            }
                            catch (Exception e)
                            {
                                LogOutMananger(e.Message, e);
                                return null;
                            }
                        });

                        temp.ReleaseObjectRunTime = new ObjectPool<OdbcConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            conn.Close();

                            return conn;
                        });


                        OdbcConnPool.Add(name, temp);
                    }
                }
            }


            return OdbcConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public ObjectPool<OdbcCommand> GetOdbcCommandInstance()
        {


            if (OdbcCommandPool == null)
            {
                OdbcCommandPool = new ObjectPool<OdbcCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                OdbcCommandPool.ReleaseObjectRunTime = new ObjectPool<OdbcCommand>.ObjectRunTimeHandle((command, pool) =>
                {
                    command.CommandText = "";
                    command.Connection = null;
                    command.CommandType = CommandType.Text;
                    command.Parameters.Clear();
                    return command;
                });

            }

            return OdbcCommandPool;
        }


        #endregion
        #endregion


        #region NPGSQL


        public Dictionary<string, ObjectPool<NpgsqlConnection>> GetNpgsqlConnectionPool()
        {
            return NpgsqlConnPool;
        }

        public ObjectPool<NpgsqlCommand> GetNpgsqlCommandPool()
        {
            return NpgsqlCommandPool;
        }

        #region configObj
        public void NpgsqlServerInstance(IEnumerable<DataConnectConfig> configuration, Action<Dictionary<string, ObjectPool<NpgsqlConnection>>, ObjectPool<NpgsqlCommand>> action)
        {
            var ConnPool = GetNpgsqlConnInstance(configuration, out int cmdPoolCount);
            var ConnCmd = GetNpgsqlCommandInstance(cmdPoolCount);
            action(ConnPool, ConnCmd);
        }


        /// <summary>
        /// 返回NpgsqlCConn
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, ObjectPool<NpgsqlConnection>> GetNpgsqlConnInstance(IEnumerable<DataConnectConfig> configuration, out int cmdPoolCount)
        {

            cmdPoolCount = -1;

            try
            {
                if (NpgsqlConnPool == null)
                {
                    NpgsqlConnPool = new Dictionary<string, ObjectPool<NpgsqlConnection>>();

                    foreach (var ar in configuration)
                    {
                        if (ar.SqlType.Equals("NPGSQL", StringComparison.OrdinalIgnoreCase))
                        {
                            string name = ar.Name;
                            string connectionstring = ar.ConnectionString;
                            if (ar.MaxCount == 0)
                                ar.MaxCount = 1;


                            if (ar.IsEncode)
                                connectionstring = DeCodeConn(connectionstring);


                            ObjectPool<NpgsqlConnection> temp = new ObjectPool<NpgsqlConnection>(ar.MaxCount);

                            cmdPoolCount += ar.MaxCount;

                            temp.TheConstructor = typeof(NpgsqlConnection).GetConstructor(new Type[] { typeof(string) });

                            temp.Param = new object[] { connectionstring };

                            temp.GetObjectRunTime = new ObjectPool<NpgsqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                try
                                {
                                    conn.Open();
                                    return conn;
                                }
                                catch (Exception e)
                                {
                                    LogOutMananger(e.Message, e);
                                    return null;
                                }
                            });

                            temp.ReleaseObjectRunTime = new ObjectPool<NpgsqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                            {
                                conn.Close();

                                return conn;
                            });


                            NpgsqlConnPool.Add(name, temp);
                        }
                    }


                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
            }



            return NpgsqlConnPool;
        }


        /// <summary>
        /// 返回NpgsqlCommand对象
        /// </summary>
        /// <returns></returns>
        private ObjectPool<NpgsqlCommand> GetNpgsqlCommandInstance(int maxCount)
        {


            if (maxCount <= 0)
                return null;
            try
            {
                if (NpgsqlCommandPool == null)
                {
                    NpgsqlCommandPool = new ObjectPool<NpgsqlCommand>(maxCount);
                    NpgsqlCommandPool.ReleaseObjectRunTime = new ObjectPool<NpgsqlCommand>.ObjectRunTimeHandle((command, pool) =>
                    {
                        command.CommandText = "";
                        command.Connection = null;
                        command.CommandType = CommandType.Text;
                        command.Parameters.Clear();
                        return command;
                    });

                }
            }
            catch (Exception er)
            {
                LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
            }



            return NpgsqlCommandPool;
        }

        #endregion


        #region ConfigApp.Config



        /// <summary>
        /// 返回NpgsqlConn
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, ObjectPool<NpgsqlConnection>> GetNpgsqlConnInstance()
        {

            if (NpgsqlConnPool == null)
            {
                NpgsqlConnPool = new Dictionary<string, ObjectPool<NpgsqlConnection>>();

                foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                {

                    if (ar.ProviderName.Equals("NpgSqlClient", StringComparison.CurrentCulture))
                    {
                        string name = ar.Name;
                        string connectionstring = ar.ConnectionString;

                        if (name.IndexOf(":") > 0)
                        {
                            string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                            if (sp.Length == 2)
                            {
                                name = sp[0];

                                if (sp[1].ToUpper() == "ENCODE")
                                {
                                    connectionstring = DeCodeConn(connectionstring);
                                }
                            }

                        }

                        ObjectPool<NpgsqlConnection> temp = new ObjectPool<NpgsqlConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                        temp.TheConstructor = typeof(NpgsqlConnection).GetConstructor(new Type[] { typeof(string) });

                        temp.Param = new object[] { connectionstring };

                        temp.GetObjectRunTime = new ObjectPool<NpgsqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            try
                            {
                                conn.Open();
                                return conn;
                            }
                            catch (Exception e)
                            {
                                LogOutMananger(e.Message, e);
                                return null;
                            }
                        });

                        temp.ReleaseObjectRunTime = new ObjectPool<NpgsqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                        {
                            conn.Close();

                            return conn;
                        });


                        NpgsqlConnPool.Add(name, temp);
                    }
                }
            }


            return NpgsqlConnPool;
        }


        /// <summary>
        /// 返回NpgsqlCommand对象
        /// </summary>
        /// <returns></returns>
        public ObjectPool<NpgsqlCommand> GetNpgsqlCommandInstance()
        {


            if (NpgsqlCommandPool == null)
            {
                NpgsqlCommandPool = new ObjectPool<NpgsqlCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                NpgsqlCommandPool.ReleaseObjectRunTime = new ObjectPool<NpgsqlCommand>.ObjectRunTimeHandle((command, pool) =>
                {
                    command.CommandText = "";
                    command.Connection = null;
                    command.CommandType = CommandType.Text;
                    command.Parameters.Clear();
                    return command;
                });

            }

            return NpgsqlCommandPool;
        }


        #endregion
        #endregion

    }
}
