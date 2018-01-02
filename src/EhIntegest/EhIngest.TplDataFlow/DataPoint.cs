using System;

namespace EhIngest.TplDataFlow
{
    public class DataPoint
    {
        public DateTime Ts { get; set; }
        public string Id { get; set; }
        public int Value { get; set; }
    }
}