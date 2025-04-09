
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

        #region GetParameters
        /*
         * -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
         *
         * Build up the parameters interface used by the Sync Engine to capture system information used to control how it connects 
         * to a target system and control its behaviour mid-flight
         * 
         * Fill in the blanks as needed:
         *  GetConnectionParameters - this is requried to allow MIM to connect to the target system
         *  GetCapabilityParameters - this is only displayed when creating the connector - usually left blank
         *  GetSchemaParameters     - if using Advanced Parameters, this can have multiple pages defined - pageNumber will start at 1 and will
         *                            increment until an empty page is returned
         * 
         * Make sure to use constants for parameter names rather than "magic" strings as these will be used in multiple places and its
         * important to make sure that we're consistent across all places where we might use them
         *
         * -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        */

        /// <summary>
        /// Generate the target system and credential parameters for the connector
        /// 
        /// Connectivity is one of the priority pages to build so we know what we're connecting to and as who
        /// for passwords, make sure we're using the CreateEncryptedStringParameter rather than CreateStringParameter so we're 
        /// taking proper care of sensitive data
        ///
        /// A basic set of parameters is setup but replace and rewrite as needed
        /// </summary>
        private IList<ConfigParameterDefinition> GetConnectivityParameters(KeyedCollection<string, ConfigParameter> configParameters)
        {
            var rv = new List<ConfigParameterDefinition>();
            //-- creates a 'starter' connection page of server, username (in domain\username format) and password with a separator
            //-- in between the server and the credential 
            //-- feel free to edit as much as needed!
            rv.Add(ConfigParameterDefinition.CreateStringParameter(SERVER, null, string.Empty));
            rv.Add(ConfigParameterDefinition.CreateStringParameter(DATABASE, null, string.Empty));        
            rv.Add(ConfigParameterDefinition.CreateDividerParameter());
            rv.Add(ConfigParameterDefinition.CreateStringParameter(USERNAME, USERNAME_REGEX, USERNAME_DEFAULT));
            rv.Add(ConfigParameterDefinition.CreateEncryptedStringParameter(PASSWORD, null));
            rv.Add(ConfigParameterDefinition.CreateDividerParameter());
            rv.Add(ConfigParameterDefinition.CreateStringParameter(VIEWNAME, null, string.Empty));
            rv.Add(ConfigParameterDefinition.CreateStringParameter(DETAILVIEW, null, string.Empty));
            rv.Add(ConfigParameterDefinition.CreateDividerParameter());
            rv.Add(ConfigParameterDefinition.CreateStringParameter(OBJECTTYPE, null));
            rv.Add(ConfigParameterDefinition.CreateStringParameter(ANCHORNAME, null));
            rv.Add(ConfigParameterDefinition.CreateStringParameter(ATTRNAME, null));
            rv.Add(ConfigParameterDefinition.CreateStringParameter(ATTRVALUE, null));
            rv.Add(ConfigParameterDefinition.CreateDividerParameter());
            rv.Add(ConfigParameterDefinition.CreateStringParameter(DETAILPAGENAME, INT_REGEX, DETAILPAGEVALUE));
            return rv;
        }

        private IList<ConfigParameterDefinition> GetCapabilityParameters(KeyedCollection<string, ConfigParameter> configParameters)
        {
            return new List<ConfigParameterDefinition>();
        }

        private IList<ConfigParameterDefinition> GetGlobalParameters(KeyedCollection<string, ConfigParameter> configParameters)
        {
            var rv = new List<ConfigParameterDefinition>();
            return rv;
        }

        private IList<ConfigParameterDefinition> GetPartitionParameters(KeyedCollection<string, ConfigParameter> configParameters)
        {
            return new List<ConfigParameterDefinition>();
        }

        private IList<ConfigParameterDefinition> GetRunStepParameters(KeyedCollection<string, ConfigParameter> configParameters)
        {
            return new List<ConfigParameterDefinition>();
        }

        /// <summary>
        /// Generates the schema pages in the MIM Sync Client
        /// 
        /// If using Advanced Parameters then pageNumber will be set to one or greater and is use to specify the page
        /// within the schema UI that is being defined - the pageNumber will continue to be incremented and the function
        /// called again until an empty parameter set is returned
        /// </summary>
        private IList<ConfigParameterDefinition> GetSchemaParameters(KeyedCollection<string, ConfigParameter> configParameters, int pageNumber = 0)
        {
            var rv = new List<ConfigParameterDefinition>();
            
            return rv;
        }
        #endregion

        #region ValidateParameters
        /// <summary>
        /// Perform validation on the provided set of parameters, page and (if using Advanced Parameters, then pageNumber will also be set
        /// </summary>        
        private ParameterValidationResult ValidateParameters(KeyedCollection<string, ConfigParameter> configParameters, ConfigParameterPage page, int pageNumber = 0)
        {
            //-- create a default 'parameter validation result' (assume we're ok)
            var rv = new ParameterValidationResult() { Code = ParameterValidationResultCode.Success };

            //-- if we encounter an error, then:
            //--    set the return code to .Failure
            //--    set ErrorParameter to the failing attribute
            //--    set ErrorMessage to explain the problem
            //--    
            //-- e.g.
            //--    rv = new ParameterValidationResult() { Code = ParameterValidationResultCode.Failure, ErrorParameter = USERNAME, ErrorMessage = "Unknown Domain" };

            return rv;
        }
        #endregion

        #region Parameter_Persistance
        /// <summary>
        /// Persistant store for parameters
        /// </summary>
        private KeyedCollection<string, ConfigParameter> Parameters { get; set; } = null;
        private Schema schema { get; set; } = null;
        private ExportRunStep ERS { get; set; }
        private OpenImportConnectionRunStep IRS { get; set; }

        /// <summary>
        /// Make sure that the parameters passed into MIM are persisted
        /// </summary>
        /// <param name="configParameters"></param>
        private void StoreParameters(KeyedCollection<string, ConfigParameter> configParameters, Schema types = null)
        {
            Parameters = configParameters;
            schema = types;
            logger.Info("-----------");
            logger.Info("-- Params");
            foreach (ConfigParameter cp in configParameters)
            {
                if (cp.IsEncrypted)
                    logger.Info("{0}:", cp.Name);
                else
                    logger.Info("{0}: {1}", cp.Name, cp.Value);
            }
            logger.Info("-----------");
        }

        private void StoreParameters(KeyedCollection<string, ConfigParameter> configParameters, Schema types, ExportRunStep ers)
        {
            ERS = ers;
            StoreParameters(configParameters, types);
        }

        private void StoreParameters(KeyedCollection<string, ConfigParameter> configParameters, Schema types, OpenImportConnectionRunStep irs)
        {
            IRS = irs;
            StoreParameters(configParameters, types);
        }

        /// <summary>
        /// Retrieve a named parameter from the persisted cache (if its set)
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        private ConfigParameter GetParameter(string parameterName)
        {
            if (Parameters == null)
            {
                throw new UnexpectedDataException("Paramaters store is null - have parameters been properly loaded yet?");
            }

            if (!Parameters.Contains(parameterName))
            {
                throw new NoSuchParameterException(parameterName);
            }

            return Parameters[parameterName];
        }
        #endregion
    }
}
