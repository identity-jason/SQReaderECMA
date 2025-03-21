
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.SqlClient;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
#if SUPPORT_IMPORT
        public int ImportDefaultPageSize => DEFAULT_PAGE_SIZE;

        public int ImportMaxPageSize => MAXIMUM_PAGE_SIZE;

        public CloseImportConnectionResults CloseImportConnection(CloseImportConnectionRunStep importRunStep)
        {
            //-- update watermark
            var cmd = PersistedConnector.CreateCommand();
            cmd.CommandText = "select CHANGE_TRACKING_CURRENT_VERSION()";
            var watermark = cmd.ExecuteScalar() as string;            
            logger.Info(watermark);
            CloseConnection();

            return new CloseImportConnectionResults(watermark);
        }

        private int start_item = 0;
        private int page_size = DEFAULT_PAGE_SIZE;
        private string watermark = null;

        public OpenImportConnectionResults OpenImportConnection(KeyedCollection<string, ConfigParameter> configParameters, Schema types, OpenImportConnectionRunStep importRunStep)
        {
            StoreParameters(configParameters, types, importRunStep);
            var rv = new OpenImportConnectionResults();

            //-- create a connection out to the target system
            //-- and persist it for later use as required
            PersistedConnector = OpenConnection();
            if (PersistedConnector!=null)
            {
                logger.Info("start item: {0}", start_item);
                logger.Info("page size: {0}", page_size);
                logger.Info("Watermark: {0}", watermark);
                start_item = 0;
                page_size = importRunStep.PageSize;
                watermark = importRunStep.CustomData;
                if (string.IsNullOrEmpty(watermark) || "OK".Equals(watermark))
                    watermark = "0";
            }

            return rv;
        }

        public GetImportEntriesResults GetImportEntries(GetImportEntriesRunStep importRunStep)
        {
#if SUPPORT_DELTA
            if (IRS.ImportType == OperationType.Full)
                return FetchImport(importRunStep);
            else
                return FetchDeltaImport(importRunStep);
#else
            return FetchImport(importRunStep);
#endif
        }
#endif


        //-- requires use of SQL change tracking
        //-- https://www.codeproject.com/Articles/1173754/Change-Tracking-Example-SQL-Server


#if SUPPORT_DELTA
        private GetImportEntriesResults FetchDeltaImport(GetImportEntriesRunStep importRunStep)
        {
            /*
select hr.*
from hr as hr
inner join CHANGETABLE(CHANGES dbo.hr, @watermark) as ct on hr.EmployeeID = ct.employeeID
order by hr.EmployeeID
offset 0 rows fetch next 10 rows only

select CHANGE_TRACKING_CURRENT_VERSION()
            */
            var rv = new List<CSEntryChange>();
            var cmd = PersistedConnector.CreateCommand();
            cmd.CommandText = string.Format("select src.*,convert(nvarchar(MAX),ct.SYS_CHANGE_VERSION) as SYS_CHANGE_VERSION from {0} as src inner join CHANGETABLE(CHANGES {0}, {4}) as ct on src.{1}=ct.{1} order by src.{1} offset {2} rows fetch next {3} rows only",
                GetParameter(VIEWNAME).Value,
                GetParameter(ANCHORNAME).Value,
                start_item,
                page_size,
                watermark);
            logger.Info(cmd.CommandText);

            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CSEntryChange csc = CSEntryChange.Create();
                csc.ObjectType = GetParameter(OBJECTTYPE).Value;
                csc.ObjectModificationType = ObjectModificationType.Add;
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    var name = dr.GetName(i);

                    if (name.Equals(GetParameter(ANCHORNAME).Value))
                    {
                        //-- we have the anchor - add it as such!
                        csc.AnchorAttributes.Add(AnchorAttribute.Create(name, dr[i] as string));
                    }
                    else if (name.Equals(SYS_CHANGE_VERSION))
                    {                        
                        watermark = dr[i] as string;
                        logger.Info("Acquiring new watermark: {0}", watermark);
                    }
                    else
                    {
                        csc.AttributeChanges.Add(AttributeChange.CreateAttributeAdd(name, dr[i] as string));
                    }
                }
                rv.Add(csc);
            }
            dr.Close();

            var has_more = false;
            start_item += rv.Count;
            if (rv.Count == page_size)
                has_more = true;

            return new GetImportEntriesResults(watermark, has_more, rv);
        }
#endif
        private GetImportEntriesResults FetchImport(GetImportEntriesRunStep importRunStep)
        {
            var rv = new List<CSEntryChange>();

            var cmd = PersistedConnector.CreateCommand();
            cmd.CommandText = string.Format("select * from {0} order by {1} offset {2} rows fetch next {3} rows only",
                GetParameter(VIEWNAME).Value,
                GetParameter(ANCHORNAME).Value,
                start_item,
                page_size);

            SqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                CSEntryChange csc = CSEntryChange.Create();
                csc.ObjectType = GetParameter(OBJECTTYPE).Value;
                csc.ObjectModificationType = ObjectModificationType.Add;
                for (int i=0;i<dr.FieldCount;i++)
                {
                    var name = dr.GetName(i);
                    if (name.Equals(GetParameter(ANCHORNAME).Value))
                    {
                        //-- we have the anchor - add it as such!
                        csc.AnchorAttributes.Add(AnchorAttribute.Create(name, dr[i] as string));
                    }
                    else
                    {
                        csc.AttributeChanges.Add(AttributeChange.CreateAttributeAdd(name, dr[i] as string));
                    }
                }
                rv.Add(csc);
            }
            dr.Close();

            var has_more = false;
            start_item += rv.Count;
            if (rv.Count == page_size)
                has_more = true;

            return new GetImportEntriesResults(watermark, has_more,rv);
        }
    }

}
