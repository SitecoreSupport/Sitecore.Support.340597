using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Azure;
using Sitecore.ContentSearch.Azure.Query;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.ContentSearch.Security;

namespace Sitecore.Support.XA.Foundation.Search.Providers.Azure
{
    public class CloudSearchProviderIndex : ContentSearch.Azure.CloudSearchProviderIndex
    {
        public CloudSearchProviderIndex(string name, string connectionStringName, string totalParallelServices, IIndexPropertyStore propertyStore) : base(name, connectionStringName, totalParallelServices, propertyStore)
        {
        }

        public override IProviderSearchContext CreateSearchContext(SearchSecurityOptions options = SearchSecurityOptions.EnableSecurityCheck)
        {
            base.CreateSearchContext(options);
            return new Sitecore.Support.XA.Foundation.Search.Providers.Azure.CloudSearchSearchContext(InitializeServiceCollectionClient(), options);
        }

        protected virtual ServiceCollectionClient InitializeServiceCollectionClient()
        {
            ServiceCollectionClient collection = new ServiceCollectionClient();

            collection.Register(typeof(ContentSearch.Azure.Query.LinqToCloudIndex<>), builder => builder.CreateInstance(typeof(Sitecore.Support.XA.Foundation.Search.Providers.Azure.LinqToCloudIndex<>), builder.TypeParameters, builder.Args));
            collection.Register(typeof(AbstractSearchIndex), builder => this);
            collection.Register(typeof(QueryStringBuilder), builder => new QueryStringBuilder(new FilterQueryBuilder(), new SearchQueryBuilder(), true));

            return collection;
        }
    }
}