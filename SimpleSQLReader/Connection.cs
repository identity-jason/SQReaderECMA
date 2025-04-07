
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Data.SqlClient;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
        //-- persist connections here
        private dynamic PersistedConnector { get; set; } = null;

        //-- persist password connections here - these need to be separate as 
        //-- passwords are potentially event-based when being driven via PCNS
        //-- so might not be called as part of the regular import/export cycle
        private dynamic PasswordConnector { get; set; } = null;
        /// <summary>
        /// Open a connection to the target system - can be called from multiple locations so is not specific to any one
        /// management agent action
        /// </summary>
        /// <returns>connection to the target system - will need to be persisted manually</returns>
        public dynamic OpenConnection()
        {
            logger.Info("Opening Connection");
            /// Make use of the persisted Parameters property rather than try to pass around the source configParameters
            /// Individual elements can be recovered using the GetParameter method we've built into the ECMA2 code.
            /// 
            /// This should be done using the constant strings we've defined rather than "magic" strings
            /// 
            /// e.g.
            ///
            bool use_impersonation = true;
            if (string.IsNullOrEmpty(GetParameter(USERNAME).Value))
            {
                logger.Info("No username provided - defaulting to service identity");
                use_impersonation = false;
            }
            else
            {
                BeginImpersonation();
            }
            logger.Info("Connecting to {0} on server {1}",
                GetParameter(DATABASE).Value,
                GetParameter(SERVER).Value);
            SqlConnection con = new SqlConnection(
                string.Format("Server={0};Initial Catalog={1};Integrated Security = SSPI; Persist Security Info = False;max pool size=10",
                GetParameter(SERVER).Value,
                GetParameter(DATABASE).Value));
            con.Open();
            if (use_impersonation)
                EndImpersonation();

            return con;
        }

        /// <summary>
        /// Dispose of the connection once we're done with it
        /// also perform any additional cleanup we might require (e.g. save out watermarks where necessary)
        /// </summary>
        public void CloseConnection()
        {
            logger.Info("Closing Connection");
            if (PersistedConnector != null)
            {                
                PersistedConnector.Close();
            }
        }


#if SUPPORT_PASSWORDS
        /// <summary>
        /// Dispose of the Password connection once we're done with it
        /// shouldn't need to perform as much additional operations as we might for a regular connection
        /// </summary>
        public void ClosePasswordConnection()
        {
            logger.Info("Closing Password Connection");
        }
#endif
    }
}
