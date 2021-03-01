using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
    public class Handler
    {
        public void Handle(Food recordModel)
        {
            Console.WriteLine(JsonConvert.SerializeObject(recordModel));

        }
    }
}
