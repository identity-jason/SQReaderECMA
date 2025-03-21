
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
            if (PersistedConnector!=null)
            {
                logger.Info("Connected - polling for schema");
                rv = new Schema();

                //-- for each object
                //-- create a new SchemaType Object
                //--    var st = SchemaType.Create(Name_Of_ObjectClass, lock_anchor_definition)
                //--    in most cases, set the anchor to be locked unless we want the end user to be able to change this 
                //--    at some point via the Sync Engine UI
                var st = SchemaType.Create(GetParameter(OBJECTTYPE).Value, true);

                //-- add the anchor attributes for the object
                st.Attributes.Add(SchemaAttribute.CreateAnchorAttribute(GetParameter(ANCHORNAME).Value, AttributeType.String));

                //-- add the additional single and multi-valued attributes and their types
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
                    st.Attributes.Add(SchemaAttribute.CreateSingleValuedAttribute(name, AttributeType.String));
                }

                //-- and make sure that we've added this into the schema itself!
                rv.Types.Add(st);

                CloseConnection();
            }

            return rv;
        }
    }
}
