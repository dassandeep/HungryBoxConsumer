using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
    public class SchemaFile
    {
        public static string GetSchemaAvro(out string schema)
        {
            string summaryQuery = string.Empty;
            var path = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location) + "\\AvroSchema\\Food.json";
            var fileStream = new FileStream(path, FileMode.Open, FileAccess.Read);
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
            {
                schema = streamReader.ReadToEnd();
            }
            return schema;
        }
        
    }
}
