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

        private string AccountEndpoint;
        private string AccountKey;
        private string DatabaseName;
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

        public async Task RunChangeFeedWithOpLogTest()
        {
            // 1. Create a new collection
            await this.CreateDocumentCollectionAsync(this.DatabaseName, this.CollectionName);

            // 2. Replace the DocumentCollection with a new RetentionDuration in its ChangeFeedPolicy
            await this.ReplaceDocumentCollectionAsync(this.DatabaseName, this.CollectionName);

            // 3. Insert 1 document and update the document as many times as specified
            await this.InsertDocumentsAndExecuteChangeFeed();

            // 4. Run ChangeFeed test with OpLog
            await this.ExecuteChangeFeedWithOpLog();
        }

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

        private async Task InsertDocumentsAndExecuteChangeFeed()
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
