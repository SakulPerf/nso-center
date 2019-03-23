using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NSOCenterUpdater
{
    public class Ea
    {
        public string Id { get; set; }
        public string MappedFileName => $"{Id}.json";
        public BsonDocument Document { get; set; }
    }
}
