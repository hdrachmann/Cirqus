using System;
using System.Collections.Generic;
using System.Linq;
using d60.Cirqus.Aggregates;
using d60.Cirqus.Events;
using d60.Cirqus.Numbers;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using NUnit.Framework;

namespace d60.Cirqus.AzureDocumentDb.Tests
{
    public class TestDatabaseQueries : AzureDocumentBase
    {
        [Test]
        public void WhenIAdd1000Doc_ThenIgetGlobalSequenceNumber()
        {
            //arrange

            //var rnd = new Random();
            //var docs = Enumerable.Range(1, 200).Select(i =>
            //                                            {
            //                                                var eventen= new RandomEvent();
            //                                                eventen.SomeInfo = rnd.NextDouble().ToString();
            //                                                eventen.Meta[GlobalSequenceNumberKey] = i;
            //                                                return eventen;
            //                                            });

            //////act



            //foreach (var doc in docs)
            //{
            //    Client.CreateDocumentAsync(Collection.SelfLink, doc).Wait();

            //}



            var sql = "select * from " + _collectionName + ".Meta." + GlobalSequenceNumberKey;

            var fetchedDocs = Client.CreateDocumentQuery(Collection.SelfLink, sql).ToList().OrderByDescending(s => s);
            var globalSq = (long)fetchedDocs.First();
            Assert.AreEqual(200, globalSq);
            //assert

        }

        [Test]
        public void WhenISaveInWrapper_ThenEachAggregateRootHasDocument()
        {
            shouldDelete = true;
            //arrange
            var rnd = new Random();
            var globalSq = 1000l;
            var docs = Enumerable.Range(1, 50).Select(i =>
                                                        {
                                                            var eventen = new RandomEvent();
                                                            eventen.SomeInfo = rnd.NextDouble().ToString();
                                                            eventen.Meta[GlobalSequenceNumberKey] = --globalSq;
                                                            return eventen;
                                                        }).ToList();
            globalSq = 0l;
            var docs2 = Enumerable.Range(1, 198).Select(i =>
                                                       {
                                                           var eventen = new RandomEvent();
                                                           eventen.SomeInfo = rnd.NextDouble().ToString();
                                                           eventen.Meta[GlobalSequenceNumberKey] = ++globalSq;
                                                           return eventen;
                                                       }).ToList();

            //act

            var aggrId = Guid.NewGuid();
            var aggr2Id = Guid.NewGuid();
            var azureBatchdoc = new AzurebatchDocument() { Id = aggrId, Events = docs.ToList() };

            Client.CreateDocumentAsync(Collection.SelfLink, azureBatchdoc).Wait();

            var azureBatchdoc2 = new AzurebatchDocument() { Id = aggr2Id, Events = docs2.ToList() };
            Client.CreateDocumentAsync(Collection.SelfLink, azureBatchdoc2).Wait();

            //assert
            var sqlInDb = "select * from " + _collectionName;// + ".Meta." + GlobalSequenceNumberKey;

            var docsInDB = Client.CreateDocumentQuery(Collection.SelfLink, sqlInDb).ToList();
            Assert.AreEqual(2,docsInDB.Count);

            var sql = "select VALUE ie.Meta." + GlobalSequenceNumberKey + " from " + _collectionName + " e JOIN ie in e.innerevents";//+ " e JOIN  ie in e.innerevents";//".Meta." + GlobalSequenceNumberKey;

           // var fetchedDocs = Client.CreateDocumentQuery(Collection.SelfLink, sql,new FeedOptions(){MaxItemCount = 1000}).OrderBy(d=>d).ToList();


            var fetchedDocs = Client.CreateDocumentQuery<AzurebatchDocument>(Collection.SelfLink, new FeedOptions() { MaxItemCount = 10 }).SelectMany(p => p.Events).OrderBy(s => s.Glele).Select(e => e.Glele).ToList();

            Assert.AreEqual(docs.Count() + docs2.Count(), fetchedDocs.Count);

            //var globalSqResult = (long)fetchedDocs.OrderByDescending(s => s).First();
            //Assert.AreEqual(globalSq, globalSqResult);

        }

        [Test]
        public void When_Then()
        {
            //arrange
            //act
            var document1 = new SomeRandonDocumentWithColl() { Id = Guid.NewGuid() };
            document1.Events = new List<Item>();
            for (int i = 0; i < 100; i++)
            {
                document1.Events.Add(new Item() { Id = i.ToString() });

            }

            Client.CreateDocumentAsync(Collection.SelfLink, document1).Wait();
            Client.Dispose();
            client = null;
            var document2 = new SomeRandonDocumentWithColl(){Id = Guid.NewGuid()};
            document2.Events = new List<Item>();
            for (int i = 100; i < 500; i++)
            {
                document2.Events.Add(new Item() { Id = i.ToString() });

            }

            Client.CreateDocumentAsync(Collection.SelfLink, document2).Wait();
            Client.Dispose();
            client = null;
            //assert
            //var sqlInDb = "select * from " + _collectionName;
          
            //var docsInDB = Client.CreateDocumentQuery(Collection.SelfLink, sqlInDb).ToList();
            //Assert.AreEqual(1, docsInDB.Count);

            var fetchedDocs = Client.CreateDocumentQuery<SomeRandonDocumentWithColl>(Collection.SelfLink,new FeedOptions(){MaxItemCount = 1000}).SelectMany(p => p.Events).Select(e => e.Id).ToList();

            Assert.AreEqual(500,fetchedDocs.Count);
        }

        public class SomeRandonDocumentWithColl
        {
            [JsonProperty(PropertyName = "id")]
            public Guid Id { get; set; }
            [JsonProperty(PropertyName = "events")]
            public List<Item> Events { get; set; }
        }

        public class Item
        {
            public string Id { get; set; }

        }
        public class AzurebatchDocument
        {
            [JsonProperty(PropertyName = "id")]
            public Guid Id { get; set; }
            [JsonProperty(PropertyName = "innerevents")]
            public List<RandomEvent> Events { get; set; }
        }


        public const string GlobalSequenceNumberKey = "gl_seq";

        public class RandomAggRoot : AggregateRoot
        {

        }
        public class RandomEvent : DomainEvent<RandomAggRoot>
        {
            public string SomeInfo { get; set; }


            public long Glele
            {
                get { return (long) Meta[GlobalSequenceNumberKey]; }
            }

        }
    }
}