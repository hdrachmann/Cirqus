using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using NUnit.Framework;

namespace d60.Cirqus.AzureDocumentDb.Tests
{
    public class AzureDocumentBase
    {
        private string _databaseName ="cirqus_tests";
        protected string _collectionName = "loseventos";
        private string _endpointUrl = "https://somedb.documents.azure.com:443/";
        private string _authorizationKey = "someKey";

        private async Task ReadOrCreateDatabase()
        {
            var databases = Client.CreateDatabaseQuery()
                .Where(db => db.Id == _databaseName).ToArray();

            if (databases.Any())
            {
                database = databases.First();
            }
            else
            {
                Database database = new Database { Id = _databaseName };
                database = await Client.CreateDatabaseAsync(database);
            }
        }
        private async Task ReadOrCreateCollection(string databaseLink)
        {
            var collections = Client.CreateDocumentCollectionQuery(databaseLink)
                .Where(col => col.Id == _collectionName).ToArray();

            if (collections.Any())
            {
                collection = collections.First();
            }
            else
            {
                collection = await Client.CreateDocumentCollectionAsync(databaseLink,
                    new DocumentCollection { Id = _collectionName });
            }
        }
        protected DocumentClient client;
        protected DocumentClient Client
        {
            get
            {
                if (client == null)
                {
                    String endpoint = _endpointUrl;
                    string authKey = _authorizationKey;
                    ;
                    Uri endpointUri = new Uri(endpoint);
                    client = new DocumentClient(endpointUri, authKey);
                }
                return client;
            }
        }
        private Database database;
        private Database Database
        {
            get
            {
                if (database == null)
                {
                    ReadOrCreateDatabase().Wait();
                }

                return database;
            }
        }

        private DocumentCollection collection;
        protected DocumentCollection Collection
        {
            get
            {
                if (collection == null)
                {
                    ReadOrCreateCollection(Database.SelfLink).Wait();
                }

                return collection;
            }
        }

        protected bool shouldDelete = true;
        [SetUp]
        protected void Setup()
        {
            _databaseName = "cirqus-tests2";
            if (shouldDelete)
            {
                try
                {

                    Client.DeleteDatabaseAsync(Database.SelfLink).Wait();
                    collection = null;
                    database = null;
                }
                catch (Exception ex)
                {
                    
                    Console.WriteLine("Error in deletion of db: " + ex.Message);
                }
            }
            ReadOrCreateDatabase().Wait();
            ReadOrCreateCollection(Database.SelfLink).Wait();

        }

        [TearDown]
        public void Teardown()
        {
            Client.Dispose();
            ;
        }
    }
}