using System;
using System.Collections.Generic;
using System.Text;

namespace ZYSQL
{
    public class DataConnectConfig
    {
        public string Name { get; set; }
        public string ConnectionString { get; set; }
        public string SqlType { get; set; }        
        public bool IsEncode { get; set; }

        public int MaxCount { get; set; }
    }
}
