
using System;
using System.IO;
using System.Xml;
using System.Text;
using System.Collections.Specialized;
using Microsoft.MetadirectoryServices;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using NLog;
using System.Security;
using NLog.LayoutRenderers.Wrappers;

/// <summary>
/// ECMA2 Template Project
/// 
/// http://www.theundocumentedsyncengine.com/content/home.html
/// 
/// Using this template
/// 
/// 1) Open the project properties and select the "Build" tab
/// 2) Locate the "Conditional compilation symbols" section on the tab and remove any of the symbols controlling features that are not needed.
///     for example, if the connector will not use a hierarchical structure then remove the 'USE_HIERARCHY' symbol from the list
///     
/// USE_HIERARCHY - controls if the connector will have a hierarchical naming structure (implies that this will be an LDAP or similar system)
/// USE_PARTITIONS - enables the connector to make use of partitions - often used in conjunction with hierarchical structures / LDAP naming
/// SUPPORT_EXPORT - Does this connector provide exports?
/// SUPPORT_IMPORT - Does this connector provide imports?
/// SUPPORT_DELTA  - Does this connector provide delta imports in addition to regular imports
/// SUPPORT_PASSWORDS - Does this connector support password operations?
/// ADVANCED_PARAMETERS - If set, allows a dynamic number of pages in the Sync Engine Config Parameters - in most cases, can be safely removed
/// 
/// In most cases, it is expected that only the SUPPORT_IMPORT and SUPPORT_EXPORT symbols will be retained
/// 
/// </summary>

namespace SimpleSQLReader
{
    public partial class EzmaExtension :
#if USE_HIERARCHY
    IMAExtensible2GetHierarchy,
#endif
#if USE_PARTITIONS
    IMAExtensible2GetPartitions,
#endif
#if SUPPORT_EXPORT
    IMAExtensible2CallExport,
#endif
#if SUPPORT_IMPORT
    IMAExtensible2CallImport,
#endif
#if SUPPORT_PASSWORDS
    IMAExtensible2Password,
#endif
    IMAExtensible2GetSchema,
#if ADVANCED_PARAMETERS
        IMAExtensible2GetParametersEx,
#else
    IMAExtensible2GetParameters,
#endif
    IMAExtensible2GetCapabilities
    {
        //-- enable NLog
        private static Logger logger = LogManager.GetCurrentClassLogger();

        //
        // Constructor
        //
        public EzmaExtension()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        //-- http://www.theundocumentedsyncengine.com/content/ecma2-capabilities.html


        private MACapabilities _macaps = null;

        public MACapabilities Capabilities
        {
            get
            {
                if (_macaps == null)
                {
                    _macaps = new MACapabilities()
                    {
                        //----------------------------
                        //-- ensure we're consistent in our use of optional features sure as hierarchy and partitions
                        //-- 
#if USE_HIERARCHY
                        SupportHierarchy = true,
#else
                        SupportHierarchy = false,
#endif
#if USE_PARTITIONS
                        SupportPartitions = true,
#else
                        SupportPartitions = false,
#endif
#if SUPPORT_EXPORT
                        //----------------------------
                        //-- SupportExport - set to true if the connector supports Export Operations OR false if it will be a read-only connector
                        //--
                        SupportExport = true,

                        //----------------------------
                        //-- Delete Add as Replace - Set true if objects are replaced by deleting them and recreating them, false otherwise
                        //-- 
                        DeleteAddAsReplace = false,

                        //----------------------------
                        //-- Controls how the connector will make updates in the target system
                        //--
                        //-- ObjectReplace -    the entire object is provided for export - ideal for systems where we drop and recreate the target object 
                        //--                    or when calling out to interfaces that require all attributes to be present
                        //-- AttributeReplace - Provides details for the attributes that are being updated but provides the entire 
                        //--                    attribute value set (group membership might get large here!)
                        //-- AttributeUpdate -  Provides only the details of changes to attributes: 
                        //--                    replacement values for SV attributes and individual adds and removes for MV attributes
                        //-- MultivaluedReferenceAttributeUpdate - hybrid of AttributeReplace and AttributeUpdate - implemented to support AzureAD so may not be needed away from this target
                        //--
                        //-- recommendation is to use Export Type in preferred order of: AttributeUpdate -> AttributeReplace -> ObjectReplace -> MultivaluedReferenceAttributeUpdate
                        //--
                        ExportType = MAExportType.AttributeUpdate,

                        //----------------------------
                        //-- FullExport controls whether the connector should present its entire content on every export.
                        //-- set to False wherever possible and only use when a requirement for a complete export of all content exists
                        //--
                        FullExport = false,

                        //----------------------------
                        //-- Turns off "Optimistic Reference Export" so references are only ever sent out as updates and never as part of an add.
                        //-- set to false where possible for speed purposes and to true if it is more effective to split the flow of references away from the initial creation
                        //--
                        NoReferenceValuesInFirstExport = false,
#else
                        SupportExport = false,
                        DeleteAddAsReplace = false,
                        ExportType = MAExportType.AttributeUpdate,
                        FullExport = false,
                        NoReferenceValuesInFirstExport = false,
#endif
#if SUPPORT_IMPORT
                        //----------------------------
                        //-- SupportImport - set to true if the connector supports import operations or false if it will be a write-only connector
                        //-- 
                        //-- recommendation - avoid write-only connectors where possible as they usually present 'challenges'
                        //--
                        SupportImport = true,

                        //----------------------------
                        //-- Delta Import - Set true if delta import is supported
                        //-- 
#if SUPPORT_DELTA
                        DeltaImport = true,
#else
                        DeltaImport = false,
#endif

#else
                        SupportImport = false,
                        DeltaImport = false,
#endif
#if SUPPORT_PASSWORDS
                        //----------------------------
                        //-- SupportPassword - does this connector support passwords - set to true if it does, false otherwise
                        //--
                        SupportPassword = true,

                        //----------------------------
                        //-- if Passwords are supported, do we export them on the initial export of an object or ship them as a follow on update
                        //--
                        ExportPasswordInFirstPass = false,
#else
                        SupportPassword = false,
                        ExportPasswordInFirstPass = false,
#endif

                        //----------------------------
                        //-- ConcurrentOperation - set true if this connector can run concurrently with others or false 
                        //--                       if it must operate as the only active connector
                        //-- 
                        ConcurrentOperation = true,

                        //----------------------------
                        //-- Sets how Distingiushed Names and Anchors behave
                        //--
                        //-- None - there is only the anchor
                        //-- Generic - the object has a DN as well as the anchor however this is non-hierarchical (flat structure) and non-partitioned
                        //-- LDAP - full LDAP Style DN structure - allows hierarchical data plus supports partitioning
                        //-- 
                        //-- recommendation is to use none in unless there is an explicit need for a generic or LDAP style DN
                        //-- 
                        DistinguishedNameStyle = MADistinguishedNameStyle.None,

                        //----------------------------
                        //-- Used in conjunction with DistinguishedNameStyle - If set to true then the DN is treated as the anchor and 
                        //-- there is no need to provide an additional identifier (e.g. ObjectID or GUID)
                        //--
                        IsDNAsAnchor = false,

                        //----------------------------
                        //-- Allows the connector to automatically normalize data (removing accents or forcing flows to uppercase) - set to None where possible unless
                        //-- the target system does not support mixed or lower case characters or has problems with accented characters.
                        //--
                        Normalizations = MANormalizations.None,

                        //----------------------------
                        //-- Controls how the connector expects to see confirming imports for objects
                        //--
                        //-- Normal - Connector expects to see a confirming import
                        //-- NoDeleteConfirmation - allows deleted objects to be missing from confirming imports
                        //-- NoAddAndDeleteConfirmation - allows the connector to skip over confirming imports for adds and deletes
                        //--
                        //-- recommendation is to use Normal wherever possible
                        //--
                        ObjectConfirmation = MAObjectConfirmation.Normal,

                        //----------------------------
                        //-- ObjectRename controls whether the connector supports the distinguished name being updated
                        //-- Set to true if using LDAP style (and potentially generic) naming or false if no DNs are in use
                        ObjectRename = false,
                    };
                }

                return _macaps;
            }
        }
    }
}
