using System.Linq;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Azure;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Linq.Common;
using Sitecore.ContentSearch.Pipelines.QueryGlobalFilters;
using Sitecore.ContentSearch.SearchTypes;
using Sitecore.ContentSearch.Security;
using Sitecore.ContentSearch.Utilities;

namespace Sitecore.Support.XA.Foundation.Search.Providers.Azure
{
    public class CloudSearchSearchContext : ContentSearch.Azure.CloudSearchSearchContext, IProviderSearchContext
    {
        private readonly ServiceCollectionClient _serviceCollectionClient;

        public CloudSearchSearchContext(ServiceCollectionClient serviceCollectionClient, SearchSecurityOptions options = SearchSecurityOptions.EnableSecurityCheck) : base(serviceCollectionClient, options)
        {
            _serviceCollectionClient = serviceCollectionClient;
        }

        IQueryable<TItem> IProviderSearchContext.GetQueryable<TItem>(params IExecutionContext[] executionContexts)
        {
            Sitecore.Support.XA.Foundation.Search.Providers.Azure.LinqToCloudIndex<TItem> linqToCloudIndex = new Sitecore.Support.XA.Foundation.Search.Providers.Azure.LinqToCloudIndex<TItem>(this, executionContexts, _serviceCollectionClient);
            if (Index.Locator.GetInstance<IContentSearchConfigurationSettings>().EnableSearchDebug())
            {
                ((IHasTraceWriter)linqToCloudIndex).TraceWriter = new LoggingTraceWriter(SearchLog.Log);
            }

            IQueryable<TItem> queryable = linqToCloudIndex.GetQueryable();
            if (typeof(TItem).IsAssignableFrom(typeof(SearchResultItem)))
            {
                QueryGlobalFiltersArgs args = new QueryGlobalFiltersArgs(queryable, typeof(TItem), executionContexts.ToList());
                Index.Locator.GetInstance<BaseCorePipelineManager>().Run("contentSearch.getGlobalLinqFilters", args);
                queryable = (IQueryable<TItem>)args.Query;
            }
            return queryable;
        }
    }
}