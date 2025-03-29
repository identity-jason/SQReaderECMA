
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.SqlClient;
using System.Diagnostics;
using Newtonsoft.Json;



namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
#if SUPPORT_IMPORT
        public int ImportDefaultPageSize => DEFAULT_PAGE_SIZE;

        public int ImportMaxPageSize => MAXIMUM_PAGE_SIZE;

        //Dictionary<string, long> WaterMark = new Dictionary<string, long>();
        Dictionary<string, Watermark> ChangeTracking = new Dictionary<string, Watermark>();
        Dictionary<string, Watermark> LastState = new Dictionary<string, Watermark>();

        public CloseImportConnectionResults CloseImportConnection(CloseImportConnectionRunStep importRunStep)
        {
            //watermark = System.Text.Json.JsonSerializer.Serialize(WaterMark);
            watermark = JsonConvert.SerializeObject(ChangeTracking);
            File.WriteAllText(MAUtils.MAFolder + @"\watermark.json", watermark);
            return new CloseImportConnectionResults(watermark);

            //return new CloseImportConnectionResults("OK");
        }

        private int start_item = 0;
        private int page_size = DEFAULT_PAGE_SIZE;
        private string watermark = null;

        public OpenImportConnectionResults OpenImportConnection(KeyedCollection<string, ConfigParameter> configParameters, Schema types, OpenImportConnectionRunStep importRunStep)
        {
            Debugger.Launch();
            StoreParameters(configParameters, types, importRunStep);
            var rv = new OpenImportConnectionResults();

            //-- create a connection out to the target system
            //-- and persist it for later use as required
            PersistedConnector = OpenConnection();
            if (PersistedConnector != null)
            {
                logger.Info("start item: {0}", start_item);
                logger.Info("page size: {0}", page_size);
                logger.Info("Watermark: {0}", watermark);
                start_item = 0;
                page_size = importRunStep.PageSize;
                watermark = importRunStep.CustomData;
                if (string.IsNullOrEmpty(watermark) || "OK".Equals(watermark))
                {
                    watermark = "OK";
                }
                else
                {
                    //WaterMark = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, long>>(watermark);
                    if (IRS.ImportType == OperationType.Full)
                    {
                        //WaterMark = new Dictionary<string, long>();
                        LastState = new Dictionary<string, Watermark>();
                        ChangeTracking = new Dictionary<string, Watermark>();
                    }
                    else
                    {
                        //WaterMark = JsonConvert.DeserializeObject<Dictionary<string, long>>(watermark);
                        LastState = JsonConvert.DeserializeObject<Dictionary<string, Watermark>>(watermark);
                        ChangeTracking = new Dictionary<string, Watermark>();
                    }
                }
            }

            return rv;
        }



        public GetImportEntriesResults GetImportEntries(GetImportEntriesRunStep importRunStep)
        {
#if SUPPORT_DELTA

            if (Everything == null)
                PreloadEverything(IRS.ImportType != OperationType.Full);

            if (IRS.ImportType == OperationType.Full)
                return FetchImport(importRunStep);
            else
                return FetchDeltaImport(importRunStep);
#else
            return FetchImport(importRunStep);
#endif
        }
#endif

#if SUPPORT_DELTA
        private GetImportEntriesResults FetchDeltaImport(GetImportEntriesRunStep importRunStep)
        {
            var rv = new List<CSEntryChange>();

            while (rv.Count < page_size && Everything.Count > 0)
            {
                bool to_add = false;

                var csc = Everything.Dequeue();
                var anchor = csc.AnchorAttributes[0].Value as string;
                var checksum = csc.GetCrc32();

                ChangeTracking.Add(csc.AnchorAttributes[0].Value as string,
    new Watermark
    {
        ObjectClass = csc.ObjectType,
        Checksum = checksum,
        DeleteMe = false
    });

                //if (ChangeTracking.ContainsKey(anchor) &&
                if (LastState.ContainsKey(anchor))
                {
                    if (ChangeTracking[anchor].Checksum != LastState[anchor].Checksum)
                    {
                        to_add = true;
                    }
                }
                else
                {
                    //WaterMark.Add(csc.AnchorAttributes[0].Value as string, csc.GetHashCode());

                    to_add = true;
                }
                if (to_add == true)
                    rv.Add(csc);
            }

#if DELTA_DELETE
            if (ToDelete != null)
            {
                while (rv.Count < page_size && ToDelete.Count > 0)
                {
                    rv.Add(ToDelete.Dequeue());
                }
            }
#endif

#if DELTA_DELETE
            var has_more = Everything.Count > 0;

            if (ToDelete != null)
                has_more = ToDelete.Count > 0 || Everything.Count > 0;
#else
            var has_more = Everything.Count>0;
#endif

            // start_item += rv.Count;
            //if (rv.Count == page_size)
            //    has_more = true;

            return new GetImportEntriesResults(watermark, has_more, rv);
        }
#endif


        private GetImportEntriesResults FetchImport(GetImportEntriesRunStep importRunStep)
        {
            var rv = new List<CSEntryChange>();

            while (rv.Count < page_size && Everything.Count > 0)
            {
                var csc = Everything.Dequeue();
                //WaterMark.Add(csc.AnchorAttributes[0].Value as string, csc.GetHashCode());
                ChangeTracking.Add(csc.AnchorAttributes[0].Value as string,
                    new Watermark
                    {
                        ObjectClass = csc.ObjectType,
                        Checksum = csc.GetCrc32()
                    });
                rv.Add(csc);
            }

            var has_more = false;
            // start_item += rv.Count;
            if (rv.Count == page_size)
                has_more = true;

            return new GetImportEntriesResults(watermark, has_more, rv);
        }
    }
}
