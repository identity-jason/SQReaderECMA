
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
        /*
        * -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
        * 
        * Routing code for Parameters - avoid making changes here and update individual methods in parameters.cs
        * 
        * -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- 
        */

        /// <summary>
        /// Build the config parameters for the nominated page
        /// </summary>
        /// <param name="configParameters"></param>
        /// <param name="page"></param>
        /// <returns></returns>
#if ADVANCED_PARAMETERS
        public IList<ConfigParameterDefinition> GetConfigParametersEx(KeyedCollection<string, ConfigParameter> configParameters, ConfigParameterPage page, int pageNumber)
#else
        public IList<ConfigParameterDefinition> GetConfigParameters(KeyedCollection<string, ConfigParameter> configParameters, ConfigParameterPage page)
#endif
        {
            switch (page)
            {
                case ConfigParameterPage.Connectivity: return GetConnectivityParameters(configParameters);

                //-- in many cases, the following pages will be left as default
                //-- but fill in any blanks as needed
                case ConfigParameterPage.Capabilities: return GetCapabilityParameters(configParameters);
                case ConfigParameterPage.Global: return GetGlobalParameters(configParameters);
                case ConfigParameterPage.Partition: return GetPartitionParameters(configParameters);
                case ConfigParameterPage.RunStep: return GetRunStepParameters(configParameters);
                case ConfigParameterPage.Schema:
#if ADVANCED_PARAMETERS
                    return GetSchemaParameters(configParameters, pageNumber);
#else
                    return GetSchemaParameters(configParameters);
#endif
            }

            //-- Should never be called but is there just in case
            return new List<ConfigParameterDefinition>();
        }

        /// <summary>
        /// Validate config parameters to ensure that we have correct information
        /// </summary>
        /// <param name="configParameters"></param>
        /// <param name="page"></param>
        /// <returns></returns>
#if ADVANCED_PARAMETERS
        public ParameterValidationResult ValidateConfigParametersEx(KeyedCollection<string, ConfigParameter> configParameters, ConfigParameterPage page,int pageNumber)
#else
        public ParameterValidationResult ValidateConfigParameters(KeyedCollection<string, ConfigParameter> configParameters, ConfigParameterPage page)
#endif
        {
#if ADVANCED_PARAMETERS
            return ValidateParameters(configParameters,page,pageNumber);
#else
            return ValidateParameters(configParameters, page);
#endif
        }
    }
}
