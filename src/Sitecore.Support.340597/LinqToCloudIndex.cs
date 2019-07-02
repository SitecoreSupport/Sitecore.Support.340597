using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Text;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Azure;
using Sitecore.ContentSearch.Azure.FieldMaps;
using Sitecore.ContentSearch.Azure.Query;
using Sitecore.ContentSearch.Azure.Schema;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Linq;
using Sitecore.ContentSearch.Linq.Common;
using Sitecore.ContentSearch.Linq.Methods;
using Sitecore.ContentSearch.Linq.Nodes;
using Sitecore.ContentSearch.Linq.Parsing;
using Sitecore.ContentSearch.Pipelines.GetFacets;
using Sitecore.ContentSearch.Pipelines.ProcessFacets;
using Sitecore.ContentSearch.Utilities;
using Sitecore.XA.Foundation.Search.Providers.Azure.Geospatial;
using ExpressionParser = Sitecore.XA.Foundation.Search.Spatial.ExpressionParser;
using CloudQueryOptimizer = Sitecore.XA.Foundation.Search.Providers.Azure.CloudQueryOptimizer;

namespace Sitecore.Support.XA.Foundation.Search.Providers.Azure
{
    public class LinqToCloudIndex<TItem> : ContentSearch.Azure.Query.LinqToCloudIndex<TItem>
    {
        private readonly CloudQueryOptimizer _queryOptimizer;

        protected Sitecore.Support.XA.Foundation.Search.Providers.Azure.CloudSearchSearchContext Context { get; }
        protected BaseCorePipelineManager PipelineManager { get; }
        protected QueryStringBuilder QueryStringBuilder;

        protected override QueryMapper<CloudQuery> QueryMapper { get; }

        protected override IQueryOptimizer QueryOptimizer => _queryOptimizer;

        public LinqToCloudIndex(Sitecore.Support.XA.Foundation.Search.Providers.Azure.CloudSearchSearchContext context, IExecutionContext executionContext) : base(context, executionContext)
        {
            ProviderIndexConfiguration config = context.Index.Configuration;
            CloudIndexParameters parameters = new CloudIndexParameters(config.IndexFieldStorageValueFormatter, config.VirtualFields, context.Index.FieldNameTranslator, typeof(TItem), false, new[] { executionContext }, context.Index.Schema);

            QueryMapper = new Sitecore.Support.XA.Foundation.Search.Providers.Azure.CloudQueryMapper(parameters);
            _queryOptimizer = new CloudQueryOptimizer();
            PipelineManager = context.Index.Locator.GetInstance<BaseCorePipelineManager>();
            QueryStringBuilder = new QueryStringBuilder(new FilterQueryBuilder(), new SearchQueryBuilder(), true);
            Context = context;
        }

        public LinqToCloudIndex(Sitecore.Support.XA.Foundation.Search.Providers.Azure.CloudSearchSearchContext context, IExecutionContext[] executionContexts, ServiceCollectionClient client) : base(context, executionContexts, client)
        {
            ProviderIndexConfiguration config = context.Index.Configuration;
            CloudIndexParameters parameters = new CloudIndexParameters(config.IndexFieldStorageValueFormatter, config.VirtualFields, context.Index.FieldNameTranslator, typeof(TItem), false, executionContexts, context.Index.Schema);
            QueryMapper = new Sitecore.Support.XA.Foundation.Search.Providers.Azure.CloudQueryMapper(parameters);
            _queryOptimizer = new CloudQueryOptimizer(); Context = context;
            PipelineManager = context.Index.Locator.GetInstance<BaseCorePipelineManager>();
            QueryStringBuilder = client.GetInstance<QueryStringBuilder>();
            Context = context;
        }

        public override IQueryable<TItem> GetQueryable()
        {
            ExpressionParser parser = new ExpressionParser(typeof(TItem), ItemType, FieldNameTranslator);
            IQueryable<TItem> queryable = new Sitecore.XA.Foundation.Search.Spatial.GenericQueryable<TItem, CloudQuery>(this, QueryMapper, QueryOptimizer, FieldNameTranslator, parser);

            foreach (IPredefinedQueryAttribute predefinedQueryAttribute in GetTypeInheritance(typeof(TItem)).SelectMany(t => t.GetCustomAttributes(typeof(IPredefinedQueryAttribute), true)).ToList())
            {
                queryable = predefinedQueryAttribute.ApplyFilter(queryable, this.ValueFormatter);
            }

            return queryable;
        }

        public override TResult Execute<TResult>(CloudQuery query)
        {
            var selectMethod = GetSelectMethod(query);
            int totalDoc, countDoc;
            Dictionary<string, object> facetResult;
            if (typeof(TResult).IsGenericType && typeof(TResult).GetGenericTypeDefinition() == typeof(SearchResults<>))
            {
                var documentType = typeof(TResult).GetGenericArguments()[0];
                var results = this.Execute(query, out countDoc, out totalDoc, out facetResult);

                var cloudSearchResultsType = typeof(CloudSearchResults<>);
                var cloudSearchResultsGenericType = cloudSearchResultsType.MakeGenericType(documentType);

                var applyScalarMethodsMethod = typeof(ContentSearch.Azure.Query.LinqToCloudIndex<TItem>).GetMethod("ApplyScalarMethods", BindingFlags.Instance | BindingFlags.NonPublic);
                var applyScalarMethodsGenericMethod = applyScalarMethodsMethod.MakeGenericMethod(typeof(TResult), documentType);

                // Execute query methods
                var processedResults = Activator.CreateInstance(cloudSearchResultsGenericType, Context, results, selectMethod, countDoc, facetResult, Parameters, query.VirtualFieldProcessors);

                return (TResult)applyScalarMethodsGenericMethod.Invoke(this, new[] { query, processedResults, totalDoc });
            }
            else
            {
                var valueList = this.Execute(query, out countDoc, out totalDoc, out facetResult);
                var processedResults = new CloudSearchResults<TItem>(Context, valueList, selectMethod, countDoc, facetResult, this.Parameters, query.VirtualFieldProcessors);

                return ApplyScalarMethods<TResult, TItem>(query, processedResults, totalDoc);
            }
        }

        public override IEnumerable<TElement> FindElements<TElement>(CloudQuery query)
        {
            if (EnumerableLinq.ShouldExecuteEnumerableLinqQuery(query))
            {
                return EnumerableLinq.ExecuteEnumerableLinqQuery<IEnumerable<TElement>>(query);
            }

            int totalDoc, countDoc;
            Dictionary<string, object> facetResult;
            var valueList = Execute(query, out countDoc, out totalDoc, out facetResult);
            var selectMethod = GetSelectMethod(query);
            var processedResults = new CloudSearchResults<TElement>(Context, valueList, selectMethod, countDoc, facetResult, Parameters, query.VirtualFieldProcessors);
            return processedResults.GetSearchResults();
        }

        protected virtual List<Dictionary<string, object>> Execute(CloudQuery query, out int countDoc, out int totalDoc, out Dictionary<string, object> facetResult)
        {
            countDoc = 0;
            totalDoc = 0;
            facetResult = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(query.Expression) || query.Methods.Count > 0)
            {
                var searchIndex = Context.Index as CloudSearchProviderIndex;
                if (searchIndex == null)
                {
                    return new List<Dictionary<string, object>>();
                }

                if (query.Expression.Contains(CloudQueryBuilder.Search.SearchForNothing))
                {
                    LogSearchQuery(query.Expression, searchIndex);
                    return new List<Dictionary<string, object>>();
                }

                // Finalize the query expression
                var expression = OptimizeQueryExpression(query, searchIndex) + "&$count=true";
                // append $count=true to find out total found document, but has performance impact;

                try
                {
                    LogSearchQuery(expression, searchIndex);
                    var results = searchIndex.SearchService.Search(expression);

                    if (string.IsNullOrEmpty(results))
                    {
                        return new List<Dictionary<string, object>>();
                    }

                    var searchResults = searchIndex.Deserializer.Deserialize(results);
                    if (searchResults.Count != 0)
                    {
                        totalDoc = searchResults.Count;
                        countDoc = totalDoc;


                        var skipFields = query.Methods.Where(m => m.MethodType == QueryMethodType.Skip).Select(m => (SkipMethod)m).ToList();
                        if (skipFields.Any())
                        {
                            var start = skipFields.Sum(skipMethod => skipMethod.Count);
                            countDoc = countDoc - start;
                        }

                        // If take method is defined, total doc will based on that
                        var takeFields = query.Methods.Where(m => m.MethodType == QueryMethodType.Take).Select(m => (TakeMethod)m).ToList();
                        if (takeFields.Any())
                        {
                            countDoc = takeFields.Sum(takeMethod => takeMethod.Count);
                            if (searchResults.Count < countDoc)
                            {
                                countDoc = searchResults.Count;
                            }
                        }

                        facetResult = searchResults.Facets;

                        return searchResults.GetRawDocuments();
                    }
                }
                catch (Exception ex)
                {
                    SearchLog.Log.Error(
                        string.Format("Azure Search Error [Index={0}] ERROR:{1} Search expression:{2}", searchIndex.Name, ex.Message, query.Expression));

                    throw;
                }
            }

            return new List<Dictionary<string, object>>();
        }

        protected virtual SelectMethod GetSelectMethod(CloudQuery compositeQuery)
        {
            var selectMethods =
                compositeQuery.Methods.Where(m => m.MethodType == QueryMethodType.Select)
                    .Select(m => (SelectMethod)m)
                    .ToList();

            return selectMethods.Count() == 1 ? selectMethods[0] : null;
        }

        [SuppressMessage("Microsoft.Design", "CA1061: Do not hide base class methods", Justification = "Base class is hide by puropus here")]
        private TResult ApplyScalarMethods<TResult, TDocument>(CloudQuery query, CloudSearchResults<TDocument> processedResults, int? totalCount)
        {
            var method = query.Methods.First();

            object result;

            switch (method.MethodType)
            {
                case QueryMethodType.All:
                    // Predicate have been applied to the where clause.
                    result = true;
                    break;
                case QueryMethodType.Any:
                    result = processedResults.Any();
                    break;
                case QueryMethodType.Count:
                    result = processedResults.Count();
                    break;
                case QueryMethodType.ElementAt:
                    result = ((ElementAtMethod)method).AllowDefaultValue
                                 ? processedResults.ElementAtOrDefault(((ElementAtMethod)method).Index)
                                 : processedResults.ElementAt(((ElementAtMethod)method).Index);
                    break;
                case QueryMethodType.GetResults:
                    var resultList = processedResults.GetSearchHits();
                    var facets = this.FormatFacetResults(processedResults.GetFacets(), query.FacetQueries);
                    var count = totalCount ?? unchecked((int)processedResults.Count());
                    result = ReflectionUtility.CreateInstance(typeof(TResult), resultList, count, facets);
                    // Create instance of SearchResults<TDocument>
                    break;
                case QueryMethodType.First:
                    result = ((FirstMethod)method).AllowDefaultValue
                                 ? processedResults.FirstOrDefault()
                                 : processedResults.First();
                    break;
                case QueryMethodType.GetFacets:
                    result = this.FormatFacetResults(processedResults.GetFacets(), query.FacetQueries);
                    break;
                case QueryMethodType.Last:
                    result = ((LastMethod)method).AllowDefaultValue
                                 ? processedResults.LastOrDefault()
                                 : processedResults.Last();
                    break;
                case QueryMethodType.Single:
                    result = ((SingleMethod)method).AllowDefaultValue
                                 ? processedResults.SingleOrDefault()
                                 : processedResults.Single();
                    break;
                default:
                    throw new InvalidOperationException("Invalid query method");
            }

            return (TResult)System.Convert.ChangeType(result, typeof(TResult));
        }

        protected virtual FacetResults FormatFacetResults(Dictionary<string, ICollection<KeyValuePair<string, int>>> facetResults, List<FacetQuery> facetQueries)
        {
            if (facetResults == null) return null;

            var fieldTranslator = Context.Index.FieldNameTranslator as CloudFieldNameTranslator;

            var pipelineArgs = new ProcessFacetsArgs(facetResults, facetQueries, facetQueries, Context.Index.Configuration.VirtualFields, fieldTranslator);
            PipelineManager.Run(Sitecore.XA.Foundation.Search.Constants.CoreContentSearchProcessFacetsPipeline, pipelineArgs);
            var processedFacets = pipelineArgs.Result;

            var facetFormattedResults = new FacetResults();

            if (fieldTranslator == null)
            {
                return facetFormattedResults;
            }

            // Filter virtual field values
            foreach (var originalQuery in facetQueries)
            {
                if (originalQuery.FieldNames.Count() > 1)
                {
                    throw new NotSupportedException("Pivot faceting is not supported by Azure Search provider.");
                }

                var fieldName = originalQuery.FieldNames.Single();

                if (!processedFacets.ContainsKey(fieldName))
                {
                    continue;
                }

                var categoryValues = processedFacets[fieldName];

                if (originalQuery.MinimumResultCount > 0)
                {
                    categoryValues = categoryValues.Where(cv => cv.Value >= originalQuery.MinimumResultCount).ToList();
                }

                var values = categoryValues.Select(v => new FacetValue(v.Key, v.Value));

                var fieldMap = (ICloudFieldMap)((CloudIndexConfiguration)Context.Index.Configuration).FieldMap;
                var fieldConfig = fieldMap.GetCloudFieldConfigurationByCloudFieldName(originalQuery.CategoryName);
                var categoryName = fieldConfig == null ? originalQuery.CategoryName : fieldConfig.FieldName;

                facetFormattedResults.Categories.Add(new FacetCategory(categoryName, values));
            }

            return facetFormattedResults;
        }

        protected virtual string OptimizeQueryExpression(CloudQuery query, CloudSearchProviderIndex index)
        {
            var expression = query.Expression;
            string facets = string.Empty;

            var facetExpression = GetFacetsExpression(query, index, out facets);

            expression = QueryStringBuilder.Merge(expression, facetExpression, "and", QueryStringBuilder.ShouldWrap.Both);

            if (QueryStringBuilder.IsSearchExpression(expression) && !expression.Contains("&queryType="))
            {
                expression += "&queryType=full";
            }

            expression = $"{expression}{facets}";

            // If 'skip' is set then set 'start' to that value.
            var skipFields = query.Methods.Where(m => m.MethodType == QueryMethodType.Skip).Select(m => (SkipMethod)m).ToList();

            if (skipFields.Any())
            {
                var start = skipFields.Sum(skipMethod => skipMethod.Count);
                expression = expression + $"&$skip={start}";
            }

            // If 'take' is set then return that number of rows.
            var takeFields = query.Methods.Where(m => m.MethodType == QueryMethodType.Take).Select(m => (TakeMethod)m).ToList();

            if (takeFields.Any())
            {
                var rows = takeFields.Sum(takeMethod => takeMethod.Count);
                expression = expression + $"&$top={rows}";
            }

            var orderByExpression = GetOrderByExpression(query, index);
            return $"{expression}{orderByExpression}";
        }

        protected virtual string GetFacetsExpression(CloudQuery query, CloudSearchProviderIndex index, out string facetPosted)
        {
            facetPosted = string.Empty;
            string expression = string.Empty;
            // If 'GetFacets' is found then add the facets into this request
            var getResultsFields = query.Methods.Where(m => m.MethodType == QueryMethodType.GetResults).Select(m => (GetResultsMethod)m).ToList();
            var facetFields = query.Methods.Where(m => m.MethodType == QueryMethodType.GetFacets).Select(m => (GetFacetsMethod)m).ToList();

            if (query.FacetQueries.Count > 0 && (facetFields.Any() || getResultsFields.Any()))
            {
                GetFacetsArgs args = new GetFacetsArgs(null, query.FacetQueries, Context.Index.Configuration.VirtualFields, Context.Index.FieldNameTranslator);
                PipelineManager.Run(Sitecore.XA.Foundation.Search.Constants.CoreContentSearchGetFacetsPipeline, args);

                var facetQueriesRaw = args.FacetQueries.ToHashSet();
                var facetQueries = new List<FacetQuery>();

                foreach (var facetQuery in facetQueriesRaw)
                {
                    if (facetQueries.Any(x => x.FieldNames.SequenceEqual(facetQuery.FieldNames)))
                    {
                        continue;
                    }

                    facetQueries.Add(facetQuery);
                }

                foreach (var facetQuery in facetQueries)
                {
                    if (!facetQuery.FieldNames.Any())
                    {
                        continue;
                    }

                    foreach (var fieldName in facetQuery.FieldNames)
                    {
                        var indexFieldName = this.FieldNameTranslator.GetIndexFieldName(fieldName);

                        var schema = (Context.Index.Schema as ICloudSearchIndexSchema);
                        var fieldSchema = schema.GetFieldByCloudName(indexFieldName);
                        if (fieldSchema == null)
                        {
                            continue;
                        }

                        var facet = $"&facet={indexFieldName}";

                        if (index.MaxTermsCountInFacet != 0)
                        {
                            facet += $",sort:count,count:{index.MaxTermsCountInFacet}";
                        }

                        if (facetQuery.FilterValues != null)
                        {
                            string facetExpression = string.Empty;
                            foreach (var filterValue in facetQuery.FilterValues)
                            {
                                if (filterValue is string)
                                {
                                    facetExpression = QueryStringBuilder.Merge(facetExpression, QueryStringBuilder.SearchQueryBuilder.Equal(indexFieldName, filterValue, 1), "or");
                                }
                                else
                                {
                                    facetExpression = QueryStringBuilder.Merge(facetExpression, QueryStringBuilder.FilterQueryBuilder.Equal(indexFieldName, filterValue, fieldSchema.Type), "or");
                                }
                            }
                            expression = QueryStringBuilder.Merge(expression, facetExpression, "and", QueryStringBuilder.ShouldWrap.Right);
                        }
                        facetPosted += facet;
                    }
                }
            }
            return expression;
        }

        protected virtual string GetOrderByExpression(CloudQuery query, CloudSearchProviderIndex index)
        {
            var sortFields = query.Methods.Where(m => m.MethodType == QueryMethodType.OrderBy).Select(m => ((OrderByMethod)m)).ToList();

            if (!sortFields.Any())
            {
                return string.Empty;
            }

            var sortDistinctFields = sortFields.GroupBy(x => x.Field).Select(x => x.First());
            var orderStringBuilder = new StringBuilder();
            foreach (var sortField in sortDistinctFields)
            {
                var fieldName = sortField.Field;
                var indexFieldName = Context.Index.FieldNameTranslator.GetIndexFieldName(fieldName, typeof(TItem));

                if (!index.SearchService.Schema.AllFieldNames.Contains(indexFieldName))
                {
                    continue;
                }

                orderStringBuilder.Append(orderStringBuilder.ToString().Contains("$orderby") ? "," : "&$orderby=");
                if (sortField is OrderByDistanceMethod)
                {
                    OrderByDistanceMethod sortByDistanceField = sortField as OrderByDistanceMethod;
                    orderStringBuilder.Append($"geo.distance({sortField.Field}, geography'Point({sortByDistanceField.Coordinates.Longitude} {sortByDistanceField.Coordinates.Latitude})')");
                }
                else
                {
                    orderStringBuilder.Append(indexFieldName);
                }
                orderStringBuilder.Append(sortField.SortDirection == SortDirection.Descending ? " desc" : string.Empty);
            }

            return orderStringBuilder.ToString();
        }

        private IEnumerable<Type> GetTypeInheritance(Type type)
        {
            yield return type;
            for (Type baseType = type.BaseType; baseType != (Type)null; baseType = baseType.BaseType)
            {
                yield return baseType;
            }
        }
    }
}