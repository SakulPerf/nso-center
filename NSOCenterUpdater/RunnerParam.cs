using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NSOCenterUpdater
{
    public class RunnerParam
    {
        public int Take { get; set; }
        public int Skip { get; set; }
        public int Id { get; set; }
        public string Name => $"Runner {Id + 1}";
    }
}
