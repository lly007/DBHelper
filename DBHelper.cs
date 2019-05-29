using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DBHelper
{
    public class DBHelper:IDisposable
    {
        /// <summary>
        /// 连接字符串
        /// </summary>
        public static string SqlConnectionText
        {
            get
            {
                //此处设置数据库连接字符串
                return "Please set Connection String here.请在此处设置你的连接字符串";
            }
        }
        /// <summary>
        /// 获取一个全新的GUID
        /// </summary>
        public static string GetGuid
        {
            get
            {
                return Guid.NewGuid().ToString();
            }
        }

        private SqlConnection connection;
        /// <summary>
        /// 获取当前实例的一个打开的连接
        /// </summary>
        public SqlConnection Connection
        {
            get
            {
                try
                {
                    if (connection == null)
                    {
                        connection = new SqlConnection(SqlConnectionText);
                        connection.Open();
                    }
                    else if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        connection.Open();
                    }
                    else if (connection.State == System.Data.ConnectionState.Broken)
                    {
                        connection.Close();
                        connection.Open();
                    }
                }
                catch (SqlException sqlException)
                {
                    connection = null;
                    MessageBox.Show("数据库连接失败，请检查数据库连接：" + sqlException.Number.ToString(), "数据库连接错误", MessageBoxButtons.OK, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button1, MessageBoxOptions.ServiceNotification);
                }

                return connection;
            }
        }
        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public void CloseConnection()
        {
            if ((connection != null) && (connection.State != ConnectionState.Closed))
            {
                connection.Close();
                connection = null;
            }
        }
        /// <summary>
        /// 从Object对象获取一个SqlCommand对象
        /// </summary>
        /// <param name="sqlCommand">要转换成SqlCommand对象的Object对象，可以是SqlCommand或者string</param>
        /// <returns>SqlCommand转换结果</returns>
        public SqlCommand GetCommandFromObject(object sqlCommand)
        {
            SqlCommand command = new SqlCommand();
            if (sqlCommand.GetType() == typeof(SqlCommand))
            {
                command = sqlCommand as SqlCommand;
                command.Connection = Connection;
            }
            else if (sqlCommand.GetType() == typeof(string))
            {
                command = new SqlCommand(sqlCommand as string, Connection);
            }
            else
            {
                throw new ArgumentException("参数类型错误,必须为SqlCommand或者string类型");
            }
            return command;
        }

        /// <summary>
        /// 同步从字符串和SqlParameter参数获得SqlCommand
        /// </summary>
        /// <param name="sqlCommandString">带有参数的SQL语句字符串</param>
        /// <param name="values">多个SqlParameter</param>
        /// <returns>获得的SqlCommand对象</returns>
        public SqlCommand GetCommandFromStringAndParams(string sqlCommandString, params SqlParameter[] values)
        {
            SqlCommand command = new SqlCommand(sqlCommandString, Connection);
            if (values.Length != 0)
            {
                command.Parameters.AddRange(values);
            }
            return command;
        }

        /// <summary>
        /// 同步获取DataTable
        /// </summary>
        /// <param name="sqlCommand">SQL Select命令,可以是SQL语句字符串，也可以是Command对象</param>
        /// <returns>返回一个DataTable</returns>
        private DataTable GetTable(object sqlCommand)
        {
            using (Connection)
            {
                SqlCommand command = GetCommandFromObject(sqlCommand);
                SqlDataAdapter sqlDataAdpter = new SqlDataAdapter();
                sqlDataAdpter.SelectCommand = command;
                DataTable dataTable = new DataTable();
                sqlDataAdpter.Fill(dataTable);
                return dataTable;
            }
        }

        /// <summary>
        /// 同步使用SQL语句字符串和多个参数获取DataTable
        /// </summary>
        /// <param name="sqlCommandString">SQL语句字符串</param>
        /// <param name="values">多个参数</param>
        /// <returns>获取的DataTable</returns>
        public DataTable GetTable(string sqlCommandString, params SqlParameter[] values)
        {
            return GetTable(GetCommandFromStringAndParams(sqlCommandString, values));
        }

        /// <summary>
        /// 同步获取DataTable
        /// </summary>
        /// <param name="safeSqlString">安全的字符串格式的SQL语句</param>
        /// <returns>获取到的DataTable</returns>
        public DataTable GetTable(string safeSqlString)
        {
            return GetTable(safeSqlString as object);
        }
        /// <summary>
        /// 同步获取DataTable
        /// </summary>
        /// <param name="sqlCommand">SqlCommand对象</param>
        /// <returns>获取到的DataTable</returns>
        public DataTable GetTable(SqlCommand sqlCommand)
        {
            return GetTable(sqlCommand as object);
        }

        /// <summary>
        /// 异步获取DataTable
        /// </summary>
        /// <param name="SqlCommand">SqlCommand对象</param>
        /// <returns>获取到的DataTable任务</returns>
        public Task<DataTable> GetTableAsync(SqlCommand SqlCommand)
        {
            return new TaskFactory().StartNew<DataTable>(new Func<object, DataTable>(GetTable), SqlCommand);

        }
        /// <summary>
        /// 异步获取DataTable
        /// </summary>
        /// <param name="safeSqlString">安全的字符串格式的SQL语句</param>
        /// <returns>获取到的DataTable任务</returns>
        public Task<DataTable> GetTableAsync(string safeSqlString)
        {
            return new TaskFactory().StartNew<DataTable>(new Func<object, DataTable>(GetTable), safeSqlString);
        }
        /// <summary>
        /// 异步使用SQL语句字符串和多个参数获取DataTable
        /// </summary>
        /// <param name="sqlCommandString">SQL语句字符串</param>
        /// <param name="values">多个参数</param>
        /// <returns>获取的DataTable</returns>
        public Task<DataTable> GetTableAsync(string sqlCommandString, params SqlParameter[] values)
        {
            return new TaskFactory().StartNew<DataTable>(new Func<object, DataTable>(GetTable), GetCommandFromStringAndParams(sqlCommandString, values));
        }


        /// <summary>
        /// 同步运行SQL指令
        /// </summary>
        /// <param name="SqlCommand">SQL Select命令,可以是SQL语句字符串，也可以是Command对象</param>
        /// <returns>受影响的行数</returns>
        private int RunCommand(object SqlCommand)
        {
            using (Connection)
            {
                SqlCommand command = GetCommandFromObject(SqlCommand);
                try
                {
                    int count = command.ExecuteNonQuery();

                    return count;
                }
                catch (SqlException sqlException)
                {
                    MessageBox.Show("运行SQL命令发生错误，错误代码：" + sqlException.Number.ToString(), "数据库连接错误", MessageBoxButtons.OK, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button1, MessageBoxOptions.ServiceNotification);
                    return -1;
                }
                finally
                {
                    CloseConnection();
                }
            }
        }
        /// <summary>
        /// 同步使用SQL语句和参数对象运行指令
        /// </summary>
        /// <param name="sqlCommandString">带有参数的SQL语句</param>
        /// <param name="values">参数对象</param>
        /// <returns>受到影响的行数</returns>
        public int RunCommand(string sqlCommandString, params SqlParameter[] values)
        {
            return RunCommand(GetCommandFromStringAndParams(sqlCommandString, values));
        }
        /// <summary>
        /// 异步使用SQL语句和参数对象运行指令
        /// </summary>
        /// <param name="sqlCommandString">带有参数的SQL语句</param>
        /// <param name="values">参数对象</param>
        /// <returns>受到影响的行数</returns>
        public Task<int> RunCommandAsync(string sqlCommandString, params SqlParameter[] values)
        {
            return new TaskFactory().StartNew<int>(new Func<object, int>(RunCommand), GetCommandFromStringAndParams(sqlCommandString, values));
        }
        /// <summary>
        /// 同步运行SQL指令
        /// </summary>
        /// <param name="sqlCommand">SqlCommand对象</param>
        /// <returns>受影响的行数</returns>
        public int RunCommand(SqlCommand sqlCommand)
        {
            return RunCommand(sqlCommand as object);
        }
        /// <summary>
        /// 同步运行安全的SQL字符串指令
        /// </summary>
        /// <param name="safeSqlString">安全的字符串格式的SQL语句</param>
        /// <returns>受影响的行数</returns>
        public int RunCommand(string safeSqlString)
        {
            return RunCommand(safeSqlString as object);
        }

        /// <summary>
        /// 异步运行安全的SQL字符串指令
        /// </summary>
        /// <param name="safeSqlString">安全的字符串格式的SQL语句</param>
        /// <returns>受影响的行数任务</returns>
        public Task<int> RunCommandAsync(string safeSqlString)
        {
            return new TaskFactory().StartNew(new Func<object, int>(RunCommand), safeSqlString);
        }
        /// <summary>
        /// 异步运行SQL指令
        /// </summary>
        /// <param name="sqlCommand">SqlCommand对象</param>
        /// <returns>受影响的行数任务</returns>
        public Task<int> RunCommandAsync(SqlCommand sqlCommand)
        {
            return new TaskFactory().StartNew(new Func<object, int>(RunCommand), sqlCommand);
        }
        /// <summary>
        /// 从object的SQL指令组转换为SqlCommand数组
        /// </summary>
        /// <param name="sqlCommands">SQL指令组</param>
        /// <returns>SqlCommand数组</returns>
        public SqlCommand[] GetCommandsFromObjects(object sqlCommands)
        {
            SqlCommand[] commands;
            try
            {
                //参数SqlCommand为SqlCommand数组时
                if (sqlCommands.GetType() == typeof(SqlCommand[]))
                {
                    commands = sqlCommands as SqlCommand[];
                    foreach (var command in commands)
                    {
                        command.Connection = Connection;
                    }
                    return commands;
                }
                //参数SqlCommand为List<SqlCommand>时
                else if (sqlCommands.GetType() == typeof(List<SqlCommand>))
                {
                    commands = (sqlCommands as List<SqlCommand>).ToArray();
                    foreach (var command in commands)
                    {
                        command.Connection = Connection;
                    }
                    return commands;
                }
                //参数SqlCommand为string数组时
                else if (sqlCommands.GetType() == typeof(string[]))
                {
                    string[] sqlCommandStrings = sqlCommands as string[];
                    commands = new SqlCommand[sqlCommandStrings.Length];
                    for (int i = 0; i < sqlCommandStrings.Length; i++)
                    {
                        commands[i] = new SqlCommand(sqlCommandStrings[i], Connection);
                    }
                    return commands;
                }
                //参数SqlCommand为List<string>时
                else if (sqlCommands.GetType() == typeof(List<string>))
                {
                    string[] sqlCommandStrings = (sqlCommands as List<string>).ToArray();
                    commands = new SqlCommand[sqlCommandStrings.Length];
                    for (int i = 0; i < sqlCommandStrings.Length; i++)
                    {
                        commands[i] = new SqlCommand(sqlCommandStrings[i], Connection);
                    }
                    return commands;
                }
                else
                {
                    throw new ArgumentException("SqlCommands参数类型错误，必须为SqlCommand数组或string数组");
                }
            }
            catch
            {
                MessageBox.Show("SQL语句组错误，请检查", "SQL语句组错误", MessageBoxButtons.OK, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button1, MessageBoxOptions.ServiceNotification);
                return null;
            }
        }

        /// <summary>
        /// 将一组SQL语句以事务方式同步运行
        /// </summary>
        /// <param name="sqlCommands">SQL语句字符串数组</param>
        /// <returns>所有SQL事务受影响行数总和</returns>        
        private int RunTransactionCommands(object sqlCommands)
        {
            //开启事务
            SqlTransaction sqlTransaction = Connection.BeginTransaction();
            SqlCommand[] commands = GetCommandsFromObjects(sqlCommands);
            if (commands != null)
            {
                try
                {
                    int count = 0;
                    if (commands.Length != 0)
                    {
                        {
                            foreach (var command in commands)
                            {
                                command.Transaction = sqlTransaction;
                                count += command.ExecuteNonQuery();
                            }
                        }
                        sqlTransaction.Commit();
                    }
                    return count;
                }
                catch (SqlException sqlException)
                {
                    sqlTransaction.Rollback();
                    MessageBox.Show("SQL事务运行错误，错误代码：" + sqlException.Number.ToString(), "数据库连接错误", MessageBoxButtons.OK, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button1, MessageBoxOptions.ServiceNotification);
                    return -1;
                }
            }
            else
            {
                return -1;
            }
        }

        /// <summary>
        /// 将一组SQL语句以事务方式同步运行
        /// </summary>
        /// <param name="sqlCommands">SqlCommand对象数组</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public int RunTransactionCommands(SqlCommand[] sqlCommands)
        {
            return RunTransactionCommands(sqlCommands as object);
        }
        /// <summary>
        /// 将一组安全的SQL语句字符串以事务方式同步运行
        /// </summary>
        /// <param name="safeSqlCommandStrings">安全的SQL语句字符串数组</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public int RunTransactionCommands(string[] safeSqlCommandStrings)
        {
            return RunTransactionCommands(safeSqlCommandStrings as object);
        }
        /// <summary>
        /// 将一组SQL语句以事务方式同步运行
        /// </summary>
        /// <param name="sqlCommandList">SqlCommand对象List</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public int RunTransactionCommands(List<SqlCommand> sqlCommandList)
        {
            return RunTransactionCommands(sqlCommandList as object);
        }
        /// <summary>
        /// 将一组SQL语句以事务方式同步运行
        /// </summary>
        /// <param name="sqlCommandStringList">SQL语句字符串List</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public int RunTransactionCommands(List<string> sqlCommandStringList)
        {
            return RunTransactionCommands(sqlCommandStringList as object);
        }

        /// <summary>
        /// 将一组SQL语句以事务方式异步运行
        /// </summary>
        /// <param name="sqlCommands">SqlCommand数组</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public Task<int> RunTransactionCommandsAsync(SqlCommand[] sqlCommands)
        {
            return new TaskFactory().StartNew<int>(new Func<object, int>(RunTransactionCommands), sqlCommands);
        }
        /// <summary>
        /// 将一组安全的SQL语句以事务方式异步运行
        /// </summary>
        /// <param name="safeSqlCommandStrings">安全的SQL语句字符串数组</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public Task<int> RunTransactionCommandsAsync(string[] safeSqlCommandStrings)
        {
            return new TaskFactory().StartNew<int>(new Func<object, int>(RunTransactionCommands), safeSqlCommandStrings);
        }
        /// <summary>
        /// 将一组SQL语句以事务方式异步运行
        /// </summary>
        /// <param name="sqlCommandList">SqlCommand对象List</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public Task<int> RunTransactionCommandsAsync(List<SqlCommand> sqlCommandList)
        {
            return new TaskFactory().StartNew<int>(new Func<object, int>(RunTransactionCommands), sqlCommandList);
        }
        /// <summary>
        /// 将一组安全的SQL语句字符串以事务方式异步运行
        /// </summary>
        /// <param name="safeSqlCommandStringList">安全的SQL语句字符串List</param>
        /// <returns>所有SQL事务受影响行数总和</returns>     
        public Task<int> RunTransactionCommandsAsync(List<string> safeSqlCommandStringList)
        {
            return new TaskFactory().StartNew<int>(new Func<object, int>(RunTransactionCommands), safeSqlCommandStringList);
        }

        public void Dispose()
        {
            Connection.Dispose();
        }
    }
}
