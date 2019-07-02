using Sitecore.ContentSearch.Azure.Query;
using Sitecore.XA.Foundation.Search.Models;
using Spatial4n.Core.Distance;

namespace Sitecore.Support.XA.Foundation.Search.Providers.Azure
{
    public class CloudQueryMapper : Sitecore.XA.Foundation.Search.Providers.Azure.CloudQueryMapper
    {
        public CloudQueryMapper(CloudIndexParameters parameters) : base(parameters)
        {
        }
        protected override string HandleWithinRadius(Sitecore.XA.Foundation.Search.Spatial.WithinRadiusNode node, CloudQueryMapperState mappingState)
        {
            double distance = node.Radius.Unit == Unit.Miles ? node.Radius.Value * DistanceUtils.MILES_TO_KM : node.Radius.Value;
            string withinDistanceQuery = $"geo.distance({node.Field}, geography'Point({node.Center.Longitude} {node.Center.Latitude})') lt {distance}";

            return QueryStringBuilder.FilterQueryBuilder.And(withinDistanceQuery);
        }
    }
}