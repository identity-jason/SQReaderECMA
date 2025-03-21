using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net.Security;
using System.Security;
using System.Data.SqlClient;
using NLog;
using System.Threading.Tasks;
using System.Management;
using System.Configuration;
using System.Runtime.InteropServices;
using System.Security.Principal;

namespace SimpleSQLReader
{
    public partial class EzmaExtension
    {
        [DllImport("advapi32.dll", SetLastError = true)]
        public static extern bool LogonUser(string lpszUsername, string lpszDomain, string lpszPassword, int dwLogonType, int dwLogonProvider, ref IntPtr phToken);

        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern int DuplicateToken(IntPtr hToken,
            int impersonationLevel,
            ref IntPtr hNewToken);

        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool RevertToSelf();

        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
        public static extern bool CloseHandle(IntPtr handle);

        private WindowsImpersonationContext _impContext = null;

        private string _impersonated_user = null;

        /// <summary>
        /// End impersonation of the user
        /// </summary>
        public void EndImpersonation()
        {
            try
            {
                if (_impContext != null)
                {
                    _impContext.Undo();
                }
            }
            catch (Exception ex)
            {
            }
        }

        public bool BeginImpersonation()
        {
            try
            {
                string username = GetParameter(USERNAME).Value;
                string[] elements = username.Split(@"\".ToCharArray(), 2);
                var _domain = elements[0];
                var _username = elements[1];
                System.Net.NetworkCredential nc = new System.Net.NetworkCredential("", GetParameter(PASSWORD).SecureValue);
                var password = nc.Password;

                _impersonated_user = string.Format("{0}\\{1}", _domain, _username);

                bool success = false;

                try
                {
                    IntPtr userHandle = IntPtr.Zero;
                    bool logonUser = LogonUser(_username, _domain,
                                        password,
                                        4,
                                        0,
                                        ref userHandle);

                    WindowsIdentity newIdentity = new WindowsIdentity(userHandle);
                    _impContext = newIdentity.Impersonate();
                    success = true;
                }
                catch (Exception ex)
                {
                    success = false;
                }

                if (!success)
                {
                    WindowsIdentity tempWindowsIdentity;
                    IntPtr token = IntPtr.Zero;
                    IntPtr tokenDuplicate = IntPtr.Zero;

                    if (RevertToSelf())
                    {
                        if (LogonUser(_username, _domain, password, 4, 0, ref token) == true)
                        {
                            if (DuplicateToken(token, 2, ref tokenDuplicate) != 0)
                            {
                                tempWindowsIdentity = new WindowsIdentity(tokenDuplicate);
                                _impContext = tempWindowsIdentity.Impersonate();
                                if (_impContext != null)
                                {
                                    CloseHandle(token);
                                    CloseHandle(tokenDuplicate);
                                    return true;
                                }
                            }
                        }
                    }

                    if (token != IntPtr.Zero)
                        CloseHandle(token);
                    if (tokenDuplicate != IntPtr.Zero)
                        CloseHandle(tokenDuplicate);
                }
            }
            catch (Exception ex)
            {
            }

            return true;
        }
    }
}

