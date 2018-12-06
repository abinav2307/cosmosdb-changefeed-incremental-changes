using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Threading;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents.Client;

using Newtonsoft.Json;

namespace Microsoft.CosmosDB.ChangeFeedTest
{
    internal sealed class ChangeFeedWithOpLogTester
    {
        private DocumentClient DocumentClient;

        /// <summary>
        /// Azure Cosmos DB account endpoint
        /// </summary>
        private string AccountEndpoint;

        /// <summary>
        /// Account Key to access the Cosmos DB account
        /// </summary>
        private string AccountKey;

        /// <summary>
        /// Name of the Cosmos DB database
        /// </summary>
        private string DatabaseName;

        /// <summary>
        /// Name of the Cosmos DB collection
        /// </summary>
        private string CollectionName;

        public ChangeFeedWithOpLogTester()
        {
            this.AccountEndpoint = ConfigurationManager.AppSettings["AccountEndpoint"];
            this.AccountKey = ConfigurationManager.AppSettings["AccountKey"];
            this.DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
            this.CollectionName = ConfigurationManager.AppSettings["CollectionName"];

            this.DocumentClient = new DocumentClient(
                new Uri(this.AccountEndpoint),
                this.AccountKey, 
                new ConnectionPolicy()
                {
                     RequestTimeout = new TimeSpan(0, 10, 0),
                     ConnectionMode = ConnectionMode.Direct,
                     ConnectionProtocol = Protocol.Tcp
                });
        }

        /// <summary>
        /// This method performs the following actions to highlight incremental changes fetched from ChangeFeed
        /// 1. Creates or re-creates the collection with the specified name
        /// 2. Replaces the RetentionDuration as part of the ChangeFeedPolicy associated with the collection
        /// 3. Inserts a document, upserts the same document a specified number of times, and finally deletes the document
        /// 4. Runs ChangeFeed to capture all the changes from (3)
        /// </summary>
        /// <returns></returns>
        public async Task RunChangeFeedWithOpLogTest()
        {
            // 1. Create a new collection
            await this.CreateDocumentCollectionAsync(this.DatabaseName, this.CollectionName);

            // 2. Replace the DocumentCollection with a new RetentionDuration in its ChangeFeedPolicy
            await this.ReplaceDocumentCollectionAsync(this.DatabaseName, this.CollectionName);

            // 3. Insert 1 document and update the document as many times as specified
            await this.InsertUpsertAndDeleteDocument();

            // 4. Run ChangeFeed test with OpLog
            await this.ExecuteChangeFeedWithOpLog();
        }

        /// <summary>
        /// Executes ChangeFeed on the specified collection to capture incremental changes provided by ChangeFeed
        /// </summary>
        /// <returns></returns>
        private async Task ExecuteChangeFeedWithOpLog()
        {
            Console.WriteLine("Waiting 20 seconds prior to executing ChangeFeed");
            Thread.Sleep(20 * 1000);

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.DatabaseName, this.CollectionName);
            string documentQueryContinuation = null;

            IDocumentQuery<Document> query = this.DocumentClient.CreateDocumentChangeFeedQuery(
                collectionUri.ToString(),
                new ChangeFeedOptions
                {
                    PartitionKeyRangeId = "0",
                    StartTime = DateTime.Now.AddMinutes(-5),
                    RequestContinuation = documentQueryContinuation,
                    MaxItemCount = 100
                });

            while (query.HasMoreResults)
            {
                FeedResponse<Object> readChangedDocuments = await query.ExecuteNextAsync<Object>();

                int changeCount = 1;    
                foreach (Object obj in readChangedDocuments)
                {
                    Console.WriteLine("{0} - {1}", changeCount, obj.ToString());
                    changeCount++;
                }
            }
        }

        /// <summary>
        /// This method performs the following actions:
        /// 1. Creates and inserts a new document into the Cosmos DB collection
        /// 2. Upserts the same document a specified number of times
        /// 3. Deletes the document
        /// </summary>
        /// <returns></returns>
        private async Task InsertUpsertAndDeleteDocument()
        {
            // Insert 1 document and update the same document 10 times
            Person person = GetSampleDocument();

            Uri documentsLink = UriFactory.CreateDocumentCollectionUri(this.DatabaseName, this.CollectionName);
            await this.DocumentClient.CreateDocumentAsync(documentsLink, person, null, true);

            // Update the document as many times as specified
            int numUpdatesForOpLogTesting = int.Parse(ConfigurationManager.AppSettings["NumUpdatesForTesting"]);
            int numCompletedUpdates = 0;
            while(numCompletedUpdates < numUpdatesForOpLogTesting)
            {
                person.Age = person.Age + 1;
                try
                {
                    await this.DocumentClient.UpsertDocumentAsync(documentsLink, person, null);
                }
                catch (DocumentClientException ex)
                {
                    Console.WriteLine("DocumentClientException thrown when attemptimg to upsert the document. Original exception was: {0}", ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception thrown when attemptimg to upsert the document. Original exception was: {0}", ex.Message);
                }

                numCompletedUpdates++;
            }

            Console.WriteLine("Completed upserting the same document {0} times", numUpdatesForOpLogTesting);

            Uri documentUri = UriFactory.CreateDocumentUri(this.DatabaseName, this.CollectionName, person.Id);
            try
            {
                RequestOptions requestOptions = new RequestOptions();
                requestOptions.PartitionKey = new PartitionKey(person.Id);

                await this.DocumentClient.DeleteDocumentAsync(documentUri, requestOptions);
                Console.WriteLine("Deleted the document\n");
            }
            catch (DocumentClientException ex)
            {
                Console.WriteLine("DocumentClientException encountered when attempting to delete the document. Original exception was: {0}", ex.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception encountered when attempting to delete the document. Original exception was: {0}", ex.Message);
            }
        }

        /// <summary>
        /// Creates a new collection with the specified name or re-creates the collection if it already exists
        /// </summary>
        /// <param name="databaseId">Name of the Cosmos DB database within which to create or re-create the collection</param>
        /// <param name="collectionId">Name of the Cosmos DB collection to create or re-create</param>
        /// <param name="deleteExistingColl">Flag indicating whether or not to delete an exisitng collection</param>
        /// <returns></returns>
        private async Task CreateDocumentCollectionAsync(
            string databaseId,
            string collectionId,
            bool deleteExistingColl = true)
        {
            Database database = (await this.DocumentClient.ReadDatabaseAsync(string.Format("/dbs/{0}", databaseId))).Resource;
            if (database != null)
            {
                Console.WriteLine("Database with resourceid: {0} retrieved", database.ResourceId);
            }

            string partitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];
            int offerThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);

            try
            {
                DocumentCollection existingColl = await this.DocumentClient.ReadDocumentCollectionAsync(string.Format("/dbs/{0}/colls/{1}", databaseId, collectionId));
                if (existingColl != null)
                {
                    if (!deleteExistingColl)
                    {
                        Console.WriteLine("Collection already present, returning...");
                    }
                    else
                    {
                        Console.WriteLine("Collection already present. Deleting collection...");
                        await this.DocumentClient.DeleteDocumentCollectionAsync(string.Format("/dbs/{0}/colls/{1}", databaseId, collectionId));
                        Console.WriteLine("Finished deleting the collection.");
                    }
                }
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    Console.WriteLine("Collection not found, continuing as normal...");
                }
                else
                    throw;
            }

            RangeIndex rangeIndexOverride1 = Index.Range(DataType.String, -1);
            RangeIndex rangeIndexOverride2 = Index.Range(DataType.Number, -1);
            SpatialIndex spatialIndexOverride = Index.Spatial(DataType.Point);
            IndexingPolicy indexingPolicy = new IndexingPolicy(rangeIndexOverride1, rangeIndexOverride2, spatialIndexOverride);

            Console.WriteLine("Creating collection..");
            ResourceResponse<DocumentCollection> createResponse = null;

            PartitionKeyDefinition pkDefn = null;
            if (partitionKey != null)
            {
                Collection<string> paths = new Collection<string>();
                paths.Add(partitionKey);
                pkDefn = new PartitionKeyDefinition() { Paths = paths };
            }
            if (pkDefn != null)
            {
                createResponse = await this.DocumentClient.CreateDocumentCollectionAsync(
                         database.SelfLink,
                         new DocumentCollection { Id = collectionId, IndexingPolicy = indexingPolicy, PartitionKey = pkDefn },
                         new RequestOptions { OfferThroughput = offerThroughput });
            }
            else
            {
                createResponse = await this.DocumentClient.CreateDocumentCollectionAsync(
                         database.SelfLink,
                         new DocumentCollection { Id = collectionId, IndexingPolicy = indexingPolicy },
                         new RequestOptions { OfferThroughput = offerThroughput});
            }

            Console.WriteLine("Successfully created the collection\n");
        }

        /// <summary>
        /// Replaces the RetentionDuration of the ChangeFeedPolicy associated with the Cosmos DB collection
        /// </summary>
        /// <param name="databaseId">>Name of the Cosmos DB database</param>
        /// <param name="collectionId">Name of the Cosmos DB collection</param>
        /// <returns></returns>
        private async Task ReplaceDocumentCollectionAsync(
            string databaseId,
            string collectionId)
        {
            DocumentCollection collection = (await this.DocumentClient.ReadDocumentCollectionAsync(string.Format("/dbs/{0}/colls/{1}", databaseId, collectionId))).Resource;

            // Update the ChangeFeedPolicy RetentionDuration
            collection.ChangeFeedPolicy.RetentionDuration = TimeSpan.FromMinutes(10);
            try
            {
                ResourceResponse<DocumentCollection> replacedCollection = await this.DocumentClient.ReplaceDocumentCollectionAsync(collection);
                Console.WriteLine("Replaced the ChangeFeedPolicy RetentionDuration for the collection.\n");
            }
            catch (DocumentClientException ex)
            {
                Console.WriteLine("DocumentClientException thrown when attempting to replace the collection's ChangeFeedPolicy RetentionDuration. Original exception was: {0}", ex.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception thrown when attempting to replace the collection's ChangeFeedPolicy RetentionDuration. Original exception was: {0}", ex.Message);
            }
        }

        /// <summary>
        /// Generates a sample POCO to insert into Cosmos DB as a document
        /// </summary>
        /// <returns></returns>
        private Person GetSampleDocument()
        {
            Person person = new Person();
            person.FirstName = "Abinav";
            person.LastName = "Rameesh";
            person.Age = 30;
            person.Employer = "Microsoft Corporation";
            person.Title = "Software Engineer";
            person.UndergraduateUniversity = "University of Michigan, Ann Arbor";
            person.PostGraduateUniversity = "Cornell University";
            person.Id = person.FirstName;

            return person;
        }
    }
}
