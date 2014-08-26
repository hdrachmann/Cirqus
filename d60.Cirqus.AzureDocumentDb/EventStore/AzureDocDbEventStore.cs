using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using d60.Cirqus.Events;
using d60.Cirqus.Exceptions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace d60.Cirqus.AzureDocumentDb.EventStore
{
    public class AzureDocDbEventStore : IEventStore
    {

        private readonly string _endpointUrl;
        private readonly string _authorizationKey;
        private readonly string _databaseName;
        private readonly string _collectionName;

        const string EventsDocPath = "Events";
        const string MetaDocPath = "Meta";

        static readonly string SeqNoDocPath = string.Format("{0}.{1}.{2}", EventsDocPath, MetaDocPath, DomainEvent.MetadataKeys.SequenceNumber);
        static readonly string GlobalSeqNoDocPath = string.Format("{0}.{1}.{2}", EventsDocPath, MetaDocPath, DomainEvent.MetadataKeys.GlobalSequenceNumber);
        static readonly string AggregateRootIdDocPath = string.Format("{0}.{1}.{2}", EventsDocPath, MetaDocPath, DomainEvent.MetadataKeys.AggregateRootId);

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
        private DocumentCollection Collection
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

        private DocumentClient client;
        private DocumentClient Client
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

        public AzureDocDbEventStore(string endpointUrl, string authorizationKey, string databaseName, string collectionName, bool createDatabase = false)
        {

            //https://somedb.documents.azure.com:443/
            
            _endpointUrl = endpointUrl;
            _authorizationKey = authorizationKey;
            _databaseName = databaseName;
            _collectionName = collectionName;



            //_database = new Database() { Id = _databaseName };
            //_eventCollection = new DocumentCollection() { Id = _collectionName };
            //using (var documentClient = new DocumentClient(new Uri(_endpointUrl), _authorizationKey, null, ConsistencyLevel.Strong))
            //{
            if (createDatabase)
            {
                ReadOrCreateDatabase().Wait();
                ReadOrCreateCollection(Database.SelfLink).Wait();
            }

            //}




        }

        public void Save(Guid batchId, IEnumerable<DomainEvent> batch)
        {

            var events = batch.ToList();

            if (!events.Any())
            {
                throw new InvalidOperationException(string.Format("Attempted to save batch {0}, but the batch of events was empty!", batchId));
            }



            var nextGlobalSeqNo = GetNextGlobalSeqNo();

            foreach (var e in events)
            {
                e.Meta[DomainEvent.MetadataKeys.GlobalSequenceNumber] = nextGlobalSeqNo++;
                e.Meta[DomainEvent.MetadataKeys.BatchId] = batchId;
            }

            EventValidation.ValidateBatchIntegrity(batchId, events);

            var doc = new AzurebatchDocument
            {
                Id = batchId,
                Events = events


            };


            Client.CreateDocumentAsync(Collection.SelfLink, doc, disableAutomaticIdGeneration: true).Wait();

        }

        private long GetNextGlobalSeqNo()
        {
            var sql = "Select * From " + _collectionName + "." + GlobalSeqNoDocPath;

            var globalSequenceNumbers = Client.CreateDocumentQuery(Collection.SelfLink,sql).OrderByDescending(s=>s);
            return 0;
        }

        public IEnumerable<DomainEvent> Load(Guid aggregateRootId, long firstSeq = 0, long limit = 2147483647)
        {
            return null;
        }

        public long GetNextSeqNo(Guid aggregateRootId)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<DomainEvent> Stream(long globalSequenceNumber = 0)
        {
            throw new NotImplementedException();
        }
    }
    public class AzurebatchDocument
    {
        [JsonProperty(PropertyName = "id")]
        public Guid Id { get; set; }
        [JsonProperty(PropertyName = "events")]
        public List<DomainEvent> Events { get; set; }
    }
}