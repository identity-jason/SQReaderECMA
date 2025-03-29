
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
        //------------------------------
        //-- Example Parameter Names
        //--
        private const string USERNAME = "User Name";
        private const string USERNAME_REGEX = null;
        private const string USERNAME_DEFAULT = "";
        private const string PASSWORD = "Password";
        private const string SERVER = "Server Name";
        private const string DATABASE = "Database";
        private const string VIEWNAME = "Parent Table or View";
        private const string DETAILVIEW = "Detail Table or View";
        private const string OBJECTTYPE = "Object Type Column";
        private const string ANCHORNAME = "Anchor Attribute Column";
        private const string ATTRNAME = "Attribute Name Column";
        private const string ATTRVALUE = "Attribute Value Column";

        //------------------------------
        //-- Default Page Sizes (for both import and export)
        //--
        private const int DEFAULT_PAGE_SIZE = 100;
        private const int MAXIMUM_PAGE_SIZE = 1000;

        private const string SYS_CHANGE_VERSION = "SYS_CHANGE_VERSION";
    }
}
