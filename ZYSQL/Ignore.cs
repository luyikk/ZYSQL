using System;
using System.Collections.Generic;
using System.Text;

namespace ZYSQL
{
   
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public sealed class Ignore : Attribute
    {
    }
}
