using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Force.Crc32;
using Microsoft.MetadirectoryServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Security.Cryptography;

namespace SimpleSQLReader
{
    public class Checksum
    {
        long checksum = 0x81726354;

        public void AddByte(byte b)
        {
            checksum = checksum ^ b;
            var t = (checksum & 0xfe000000) >> 25;
            checksum = checksum << 7;
            checksum = checksum | t;
            checksum = checksum & 0xffffffff;
        }

        public void AddText(string text)
        {
            foreach (char c in text)
            {
                AddByte((byte)c);
            }
        }

        public long GetChecksum()
        {
            return checksum;
        }
    }

    public static class CheckSumHelper
    {
        public static long GetCrc32(this CSEntryChange csc)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(csc.AnchorAttributes[0].Name);
            sb.Append(csc.AnchorAttributes[0].Value);
            sb.Append(csc.ObjectType);
            foreach (var attr in csc.AttributeChanges)
            {
                sb.Append(attr.Name);
                attr.ValueChanges.ToList().ForEach(x => sb.Append(x));
            }

            var output = sb.ToString();

            Checksum csc32 = new Checksum();
            csc32.AddText(output);

            //byte[] res = crc32.TransformFinalBlock(Encoding.UTF8.GetBytes(output), 0, output.Length);



            /*
            using (MemoryStream ms = new MemoryStream())
            {
                BinaryFormatter bf = new BinaryFormatter();
                bf.Serialize(ms, sb.ToString());                
                crc32.Append(ms);                
            }*/

            return csc32.GetChecksum();
        }
    }
}
