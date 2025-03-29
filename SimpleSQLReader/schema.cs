
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.SqlClient;
using System.Linq;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
        private Dictionary<string,SchemaType>GetObjectTypes()
        {
            SqlCommand cmd = PersistedConnector.CreateCommand();
            cmd.CommandText = string.Format("select distinct {0} from {1}", GetParameter(OBJECTTYPE).Value, GetParameter(VIEWNAME).Value);
            logger.Info(cmd.CommandText);
            SqlDataReader dr = cmd.ExecuteReader();
            Dictionary<string, SchemaType> rv = new Dictionary<string, SchemaType>();
            while (dr.Read())
            {
                string object_type = dr[GetParameter(OBJECTTYPE).Value] as string;
                logger.Info("Detected object type: {0}", object_type);
                //-- for each object
                //-- create a new SchemaType Object
                //--    var st = SchemaType.Create(Name_Of_ObjectClass, lock_anchor_definition)
                //--    in most cases, set the anchor to be locked unless we want the end user to be able to change this 
                //--    at some point via the Sync Engine UI
                rv.Add(object_type, SchemaType.Create(object_type, true));
                //-- add the anchor attributes for the object
                rv[object_type].Attributes.Add(SchemaAttribute.CreateAnchorAttribute(GetParameter(ANCHORNAME).Value, AttributeType.String));
            }
            dr.Close();

            return rv;
        }

        /// <summary>
        /// Retrieve the schema for use in the connector
        /// </summary>
        /// <param name="configParameters"></param>
        /// <returns></returns>
        public Schema GetSchema(KeyedCollection<string, ConfigParameter> configParameters)
        {
            logger.Info("Retrieving the schema");
            Schema rv = null;
            logger.Info("Store Params");
            StoreParameters(configParameters);
            logger.Info("Open connection");
            PersistedConnector = OpenConnection();
            if (PersistedConnector != null)
            {
                logger.Info("Connected - polling for schema");
                rv = new Schema();

                //-- obtain the different object types in the schema by querying the primary view
                var ObjectTypes = GetObjectTypes();
                                
                //-- var st = SchemaType.Create(GetParameter(OBJECTTYPE).Value, true);                
                //st.Attributes.Add(SchemaAttribute.CreateAnchorAttribute(GetParameter(ANCHORNAME).Value, AttributeType.String));

                //-- add the additional single and their types
                SqlCommand cmd = PersistedConnector.CreateCommand();
                cmd.CommandText = string.Format("select top(1) * from {0}", GetParameter(VIEWNAME).Value);
                logger.Info(cmd.CommandText);
                SqlDataReader dr = cmd.ExecuteReader();
                dr.Read();
                for (int i=0;i<dr.FieldCount;i++)
                {
                    var name = dr.GetName(i);
                    var data = dr.GetDataTypeName(i);

                    if (name.Equals(GetParameter(ANCHORNAME).Value))
                        continue;

                    logger.Info("Located field with name: {0} and SQL Data Type of: {1}", name, data);
                    //st.Attributes.Add(SchemaAttribute.CreateSingleValuedAttribute(name, AttributeType.String));
                    ObjectTypes.Keys.ToList().ForEach(k => ObjectTypes[k].Attributes.Add(SchemaAttribute.CreateSingleValuedAttribute(name, AttributeType.String)));
                }
                dr.Close();

                //-- Now we'll run through the detail View to recover the MV schema
                cmd = PersistedConnector.CreateCommand();
                cmd.CommandText = string.Format("select distinct({0}) from {1}", GetParameter(ATTRNAME).Value, GetParameter(DETAILVIEW).Value);
                logger.Info(cmd.CommandText);
                dr = cmd.ExecuteReader();
                var mvattrs = new List<string>();
                while (dr.Read())
                {
                    mvattrs.Add(dr[GetParameter(ATTRNAME).Value] as string);
                    
                }
                dr.Close();

                foreach (string attr in mvattrs)
                {
                    cmd = PersistedConnector.CreateCommand();
                    /*
select top(1) *
from dbo.vw_detail
where attribute_name = 'member'
and attribute_value is not null
                    */

                    //cmd.CommandText = string.Format("select top(1) {0} from {1} where {0} is not null", attr, GetParameter(DETAILVIEW).Value);

                    cmd.CommandText = string.Format("select top(1) {0} from {1} where {2}='{3}' and {0} is not null",
                        GetParameter(ATTRVALUE).Value, 
                        GetParameter(DETAILVIEW).Value, 
                        GetParameter(ATTRNAME).Value, 
                        attr);

                    logger.Info(cmd.CommandText);
                    dr = cmd.ExecuteReader();
                    dr.Read();
                    var sample = dr[GetParameter(ATTRVALUE).Value] as string;
                    dr.Close();

                    logger.Info("located MV field with name: {0}", attr);
                    if (sample.StartsWith("{") &&
                        sample.Length == 38 &&
                        sample.EndsWith("}"))
                    {
                        ObjectTypes.Keys.ToList().ForEach(k => ObjectTypes[k].Attributes.Add(SchemaAttribute.CreateMultiValuedAttribute(attr, AttributeType.Reference)));
                    }
                    else
                    {
                        ObjectTypes.Keys.ToList().ForEach(k => ObjectTypes[k].Attributes.Add(SchemaAttribute.CreateMultiValuedAttribute(attr, AttributeType.String)));
                    }
                }


                //-- and make sure that we've added this into the schema itself!
                //rv.Types.Add(st);
                ObjectTypes.Keys.ToList().ForEach(k => rv.Types.Add(ObjectTypes[k]));

                CloseConnection();
            }

            return rv;
        }
    }
}
