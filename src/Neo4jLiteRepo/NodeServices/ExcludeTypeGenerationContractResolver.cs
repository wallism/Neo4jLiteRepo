using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.NodeServices
{
    /// <summary>
    /// Custom Contract Resolver to ignore $type generation (TypeNameHandling.Auto)
    /// for properties decorated with NodeRelationshipAttribute or ExcludeFromTypeGenerationAttribute
    /// </summary>
    internal class ExcludeTypeGenerationContractResolver : Newtonsoft.Json.Serialization.DefaultContractResolver
    {
        protected override IList<Newtonsoft.Json.Serialization.JsonProperty> CreateProperties(Type type, Newtonsoft.Json.MemberSerialization memberSerialization)
        {
            var properties = base.CreateProperties(type, memberSerialization);

            foreach (var property in properties)
            {
                // Get the corresponding property info from the type
                var propertyInfo = type.GetProperty(property.UnderlyingName);

                if (propertyInfo != null)
                {
                    // Check if the property has the NodeRelationship or ExcludeFromTypeGenerationAttribute
                    if (propertyInfo.GetCustomAttributes(typeof(NodeRelationshipAttribute<>), true).Any() ||
                        propertyInfo.GetCustomAttributes(typeof(ExcludeFromTypeGenerationAttribute), true).Any())
                    {
                        property.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.None; // Disable $type for this property
                    }
                }
            }

            return properties;
        }
    }
}
