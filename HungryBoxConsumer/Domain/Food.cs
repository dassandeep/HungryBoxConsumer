using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HungryBoxConsumer
{
    public class Food
    {
        public string ItemName { get; set; }
        public string ItemPrice { get; set; }
        public string HotelName { get; set; }
        //public FoodType foodType { get; set; }
    }
    public enum FoodType
    {
        Italian,
        French,
        Greek,
        Turkish,
        Azian
    }
}
