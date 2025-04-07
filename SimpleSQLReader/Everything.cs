using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.ServiceModel.Syndication;
using System.Text;
using System.Threading.Tasks;
using Microsoft.MetadirectoryServices;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {

        Queue<CSEntryChange> Everything = null;
        Dictionary<string, CSEntryChange> EverythingIndex = null;

        Queue<CSEntryChange> ToDelete = null;

        internal string attrname { get; set; }
        internal string attrvalue { get; set; }
        internal string anchorname { get; set; }

        void PreloadEverything(bool isDelta = false)
        {
            logger.Info("Preloading Data from SQL");
            
            attrname = GetParameter(ATTRNAME).Value;
            attrvalue = GetParameter(ATTRVALUE).Value;
            anchorname = GetParameter(ANCHORNAME).Value;

            logger.Info("Anchor: {0}", anchorname);
            logger.Info("Attribute Name: {0}", attrname);
            logger.Info("Attribute Value: {0}", attrvalue);
            
            EverythingIndex = new Dictionary<string, CSEntryChange>();

            LoadPrimaryView(isDelta);

            logger.Info("Load and merge detail view");
            LoadDetailView();

            logger.Info("Convert Index to Queue for loading");
            Everything = new Queue<CSEntryChange>(EverythingIndex.Values);
            EverythingIndex = null;

            logger.Info("{0} item staged for import", Everything.Count);

            start_item = 0;
        }

        internal void LoadDetailView()
        {
            logger.Info("Load detail view");
            int details = 0;
            start_item = 0;
            Dictionary<string, Dictionary<string, List<object>>> detailcache = new Dictionary<string, Dictionary<string, List<object>>>();
            do
            {
                var cmd = PersistedConnector.CreateCommand();
                logger.Info("Loading Detail Page from SQL");
                cmd.CommandText = string.Format("select * from {0} order by {1} offset {2} rows fetch next {3} rows only",
                    GetParameter(DETAILVIEW).Value,
                    anchorname,
                    start_item,
                    page_size);

                logger.Info(cmd.CommandText);

                SqlDataReader dr = cmd.ExecuteReader();
                details = 0;
                while (dr.Read())
                {
                    details++;
                    string anchor = dr[anchorname] as string;
                    string attr = dr[attrname] as string;
                    string val = dr[attrvalue] as string;

                    //--logger.Trace("{0},{1},{2}", anchor, attr, val);

                    if (!detailcache.ContainsKey(anchor))
                    {
                        detailcache.Add(anchor, new Dictionary<string, List<object>>());
                    }

                    if (detailcache[anchor].ContainsKey(attr))
                    {
                        detailcache[anchor][attr].Add(val);
                    }
                    else
                    {
                        detailcache[anchor].Add(attr, new List<object> { val });
                    }
                }

                start_item += page_size;
                dr.Close();
            } while (details == page_size);

            logger.Info("Merging Details into primary view");

            foreach (string anchor in detailcache.Keys)
            {
                if (EverythingIndex.ContainsKey(anchor))
                {
                    foreach (string attr in detailcache[anchor].Keys)
                    {
                        if (ChangeTracking.ContainsKey(anchor))
                        {
                            ChangeTracking[anchor].AddText(attr);
                        }

                        if (detailcache[anchor][attr].Count > 1)
                        {
                            EverythingIndex[anchor].AttributeChanges.Add(
                                AttributeChange.CreateAttributeAdd(attr, detailcache[anchor][attr]));

                            if (ChangeTracking.ContainsKey(anchor))
                            {
                                foreach (var val in detailcache[anchor][attr])
                                {
                                    ChangeTracking[anchor].AddText(val as string);
                                }
                            }
                        }
                        else
                        {
                            EverythingIndex[anchor].AttributeChanges.Add(
                         AttributeChange.CreateAttributeAdd(attr, detailcache[anchor][attr][0]));
                            if (ChangeTracking.ContainsKey(anchor))
                            {
                                ChangeTracking[anchor].AddText(detailcache[anchor][attr][0] as string);
                            }
                        }

                    }
                }
            }

            start_item = 0;
        }

        internal void LoadPrimaryView(bool isDelta)
        {
            logger.Info("Load primary view");

            int counter = 0;
            do
            {
                counter = 0;
                var cmd = PersistedConnector.CreateCommand();

                cmd.CommandText = string.Format("select * from {0} order by {1} offset {2} rows fetch next {3} rows only",
                    GetParameter(VIEWNAME).Value,
                    anchorname,
                    start_item,
                    page_size);

                logger.Info("Executing SQL to load next page of data:");
                logger.Info(cmd.CommandText);

                SqlDataReader dr = cmd.ExecuteReader();

                while (dr.Read())
                {
                    string anchor = string.Empty;
                    CSEntryChange csc = CSEntryChange.Create();
                    csc.ObjectType = dr[GetParameter(OBJECTTYPE).Value] as string;
                    csc.ObjectModificationType = ObjectModificationType.Add;
                    for (int i = 0; i < dr.FieldCount; i++)
                    {

                        var name = dr.GetName(i);
                        if (name.Equals(anchorname, StringComparison.CurrentCultureIgnoreCase))
                        {
                            //-- we have the anchor - add it as such!
                            csc.AnchorAttributes.Add(AnchorAttribute.Create(name, dr[i]));
                            anchor = dr[i] as string;
                        }
                        else
                        {
                            if (dr[i] == null)
                            {
                                csc.AttributeChanges.Add(AttributeChange.CreateAttributeAdd(name, dr[i] as string));
                            }
                        }
                    }

                    if (string.IsNullOrEmpty(anchor) == false)
                    {
                        EverythingIndex.Add(anchor, csc);

#if DELTA_DELETE
                        if (isDelta)
                        {
                            if (ChangeTracking.ContainsKey(anchor))
                            {
                                ChangeTracking[anchor].DeleteMe = false;
                            }
                        }
#endif
                    }
                    else
                    {
                        logger.Warn("No Anchor Detected: {0}", dr[0]);
                    }

                    counter++;
                }

                start_item += page_size;
                dr.Close();
                //} while (EverythingIndex.Count % page_size == 0);
            } while (counter == page_size);

            if (isDelta)
            {
                LastState.Keys.ToList().ForEach(key =>
                {
                    if (key != null)
                    {
                        if (ChangeTracking.ContainsKey(key) == false)
                        {
                            //-- item exists in last state but not this one - therefore is a delete
                            var t = CSEntryChange.Create();
                            t.ObjectType = LastState[key].ObjectClass;
                            t.AnchorAttributes.Add(AnchorAttribute.Create(anchorname, key));
                            t.ObjectModificationType = ObjectModificationType.Delete;
                        }
                    }
                });
            }

            start_item = 0;
        }
    }
}
