using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace SimpleSQLReader
{
    
    [Serializable]
    public class Watermark
    {
        private bool deleteMe = true;
        private Checksum _checksum = new Checksum();

        public string ObjectClass { get; set; }
        public long Checksum { get; set; }

        [JsonIgnore]
        public bool DeleteMe { get => deleteMe; set => deleteMe = value; }

        public long GetHash()
        {
            return _checksum.GetChecksum();
        }
        public void AddText(string text)
        {
            _checksum.AddText(text);
        }
    }

  
}
