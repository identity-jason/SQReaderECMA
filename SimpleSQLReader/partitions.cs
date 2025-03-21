
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
#if USE_PARTITIONS
        /// <summary>
        /// Return partition information from the target system
        /// </summary>
        /// <param name="configParameters"></param>
        /// <returns></returns>
        public IList<Partition> GetPartitions(KeyedCollection<string, ConfigParameter> configParameters)
        {
            throw new NotImplementedException();
        }
#endif
    }
}
