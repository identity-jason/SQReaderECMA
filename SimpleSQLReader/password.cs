
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Security;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
#if SUPPORT_PASSWORDS
        /// <summary>
        /// Open a connection for use with the password connector
        /// </summary>
        /// <param name="configParameters"></param>
        /// <param name="partition"></param>
        public void OpenPasswordConnection(KeyedCollection<string, ConfigParameter> configParameters, Partition partition)
        {
            //-- create a connection out to the target system
            //-- and persist it for later use as required
            //-- 
            //-- note we're useing PasswordConnector here instead of PersistedConnector
            //-- to ensure that we don't mix PCNS traffic with sync-cycle activity
            PasswordConnector = OpenConnection();
            if (PasswordConnector != null)
            {

            }
        }        

        /// <summary>
        /// Return a flag showing whether the connection is encrypted (secure) or not.
        /// This should be set to secure wherever possible (we should always use secure connections for password operations)
        /// </summary>
        /// <returns></returns>
        public ConnectionSecurityLevel GetConnectionSecurityLevel()
        {
            return ConnectionSecurityLevel.Secure;
        }

        /// <summary>
        /// Perform a password change operation
        /// </summary>
        /// <param name="csentry"></param>
        /// <param name="oldPassword"></param>
        /// <param name="newPassword"></param>
        public void ChangePassword(CSEntry csentry, SecureString oldPassword, SecureString newPassword)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Set a password
        /// </summary>
        /// <param name="csentry"></param>
        /// <param name="newPassword"></param>
        /// <param name="options"></param>
        public void SetPassword(CSEntry csentry, SecureString newPassword, PasswordOptions options)
        {
            throw new NotImplementedException();
        }
#endif
    }
}
