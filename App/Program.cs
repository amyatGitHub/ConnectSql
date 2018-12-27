using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace App
{
    class Program
    {
        static void Main(string[] args)
        {
            WriteData();
        }

        static void WriteData()
        {
            Person[] inputDataList = new Person[] { };
            string[] columns = new string[] { nameof(Person.Name), nameof(Person.Age) };
            string dbName = "dbName";
            string tableName = "tableName";

            ConnectSql.ConnectSql sqlConn = new ConnectSql.ConnectSql();
            sqlConn.StartConnection(ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString, dbName);
            sqlConn.InsertDB(inputDataList, columns, tableName);
            sqlConn.CloseConnection();
        }
    }

    class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
