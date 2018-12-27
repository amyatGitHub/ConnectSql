using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace ConnectSql
{
    public class ConnectMySQL
    {
        MySqlConnection objConn;
        string dbName;
        public List<string> logMsg;
        int maxParaNum = 65534;

        public void StartConnection(string sConnectionString, string dbName)
        {
            this.dbName = dbName;
            objConn = new MySqlConnection(sConnectionString);
            objConn.Open();
        }

        public void CloseConnection()
        {
            objConn.Close();
        }

        private void GetColumns(string fullTableName, Dictionary<string, int> columns)
        {
            fullTableName = fullTableName.Replace("].[", ",").Replace("[", "").Replace("]", "");
            string[] restrictions = fullTableName.Split(',');
            DataTable columnSchema;

            // Open the schema information for the columns.
            columnSchema = objConn.GetSchema("Columns", restrictions);

            if (columnSchema.Rows.Count == 0)
            {
                restrictions = new string[4] { null, null, fullTableName, null };
                columnSchema = objConn.GetSchema("Columns", restrictions);
            }

            // Enumerate the table's columns.
            foreach (DataRow row in columnSchema.Rows)
            {
                if (!columns.ContainsKey(row["COLUMN_NAME"].ToString()))
                {
                    int length;
                    if (Int32.TryParse(row["CHARACTER_MAXIMUM_LENGTH"].ToString(), out length))
                    {
                        if (length > 0)
                        {
                            // Retrieve the column name.
                            columns.Add(row["COLUMN_NAME"].ToString(), length);
                        }
                    }
                }
            }
        }

        public long InsertDB(object[] dataList, string[] tableCols, string tableName, bool autoTrun = true)
        {
            Dictionary<string, int> colInfo = new Dictionary<string, int> { };
            int eachRound;
            bool proceed = false;
            logMsg = new List<string> { };
            List<int> overlongRow = new List<int> { };
            long id = 0;

            eachRound = Convert.ToInt32(maxParaNum / (tableCols.Length));
            if ((eachRound - (maxParaNum / tableCols.Length)) > 0)
                eachRound = eachRound - 1;

            GetColumns(tableName, colInfo);

            for (int i = 0; i < dataList.Length; i = i + eachRound)
            {
                string sSQL = "INSERT INTO " + tableName + " (";

                try
                {
                    MySqlCommand objCmd = new MySqlCommand();
                    objCmd.Connection = objConn;

                    //loop table column
                    foreach (var col in tableCols)
                    {
                        sSQL += "`" + col.Trim() + "`,";
                    }
                    sSQL = sSQL.Substring(0, sSQL.Length - 1);
                    sSQL += ") VALUES ";

                    //loop data
                    for (int j = 0; j < eachRound; j++)
                    {
                        if (i + j >= dataList.Length)
                            break;

                        PropertyInfo[] properties = dataList[i + j].GetType().GetProperties();

                        //check if too long
                        bool valid = true;
                        if (!autoTrun || true)
                        {
                            for (int k = 0; k < tableCols.Length; k++)
                            {
                                string col = tableCols[k].Trim();
                                foreach (PropertyInfo property in properties)
                                {
                                    if (col == property.Name && colInfo.ContainsKey(col))
                                    {
                                        if (property.PropertyType == typeof(String))
                                        {
                                            string value = (string)(property.GetValue(dataList[i + j]));
                                            if (!string.IsNullOrEmpty(value))
                                            {
                                                if (value.Length > colInfo[col])
                                                {
                                                    Logger.Logger.WriteLog(value + " is too long for " + col);
                                                    logMsg.Add(value + " is too long for " + col);

                                                    if (!autoTrun)
                                                    {
                                                        valid = false;
                                                    }
                                                    else
                                                    {
                                                        string cutValue = value.Substring(0, colInfo[col] - 1);
                                                        property.SetValue(dataList[i + j], cutValue);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (!valid)
                            continue;

                        proceed = true;

                        //add sql
                        sSQL += "(";
                        foreach (var col in tableCols)
                        {
                            sSQL += "@" + col + "_" + j.ToString() + ",";
                        }
                        sSQL = sSQL.Substring(0, sSQL.Length - 1);
                        sSQL += "),";

                        //bind parameters
                        for (int k = 0; k < tableCols.Length; k++)
                        {
                            foreach (PropertyInfo property in properties)
                            {
                                if (tableCols[k] == property.Name)
                                {
                                    object value = property.GetValue(dataList[i + j]);
                                    if (value != null)
                                    {
                                        objCmd.Parameters.AddWithValue((tableCols[k] + "_" + j.ToString()), value);
                                    }
                                    else
                                    {
                                        objCmd.Parameters.AddWithValue((tableCols[k] + "_" + j.ToString()), DBNull.Value);
                                    }
                                }
                            }
                        }
                    }

                    //execute
                    if (proceed)
                    {
                        sSQL = sSQL.Substring(0, sSQL.Length - 1);
                        objCmd.CommandText = sSQL;
                        objCmd.ExecuteNonQuery();
                        if(i == 0)
                            id = objCmd.LastInsertedId;
                    }
                }
                catch (Exception e)
                {
                    // Get stack trace for the exception with source file information
                    var st = new System.Diagnostics.StackTrace(e, true);
                    // Get the top stack frame
                    var frame = st.GetFrame(0);
                    // Get the line number from the stack frame
                    var line = frame.GetFileLineNumber();
                    Logger.Logger.WriteLog(e);
                }
            }

            return id;
        }
    }
}
