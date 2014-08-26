using d60.Cirqus.AzureDocumentDb.EventStore;
using d60.Cirqus.Events;

namespace d60.Cirqus.Tests.Contracts.EventStore.Factories
{
    public class AzureDocumentDbFactory:IEventStoreFactory
    {
        private readonly AzureDocDbEventStore eventStore;
        public AzureDocumentDbFactory()
        {
            eventStore = new AzureDocDbEventStore("https://somedb.documents.azure.com:443/", "someKey","events-tests","events",true);
        }
        public IEventStore GetEventStore()
        {
            return eventStore;

        }
    }
}