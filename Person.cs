using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace Microsoft.CosmosDB.ChangeFeedTest
{
    /// <summary>
    /// POCO for generating a sample document to insert, update and delete in Azure Cosmos DB
    /// </summary>
    internal sealed class Person
    {
        [JsonProperty(PropertyName = "firstName")]
        public string FirstName { get; set; }

        [JsonProperty(PropertyName = "lastName")]
        public string LastName { get; set; }

        [JsonProperty(PropertyName = "employer")]
        public string Employer { get; set; }

        [JsonProperty(PropertyName = "title")]
        public string Title { get; set; }

        [JsonProperty(PropertyName = "age")]
        public int Age { get; set; }

        [JsonProperty(PropertyName = "undergraduateUniversity")]
        public string UndergraduateUniversity { get; set; }

        [JsonProperty(PropertyName = "postGraduateUniversity")]
        public string PostGraduateUniversity { get; set; }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }        
    }
}
