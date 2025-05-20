namespace Node.Training
{
    /// <summary>
    /// By default, child classes are 'flattened' into the parent class.
    /// Add items to the config with this structure to create separate nodes for the child property.
    /// </summary>
    public class AlwaysSeparateNodeProperty()
    {
        public string Name { get; set; } = "";
        /// <summary>
        /// Required if the class does not have a Name property.
        /// </summary>
        public string? DisplayName { get; set; }
        /// <summary>
        /// Override the name of the generated nested class.
        /// </summary>
        public string? ClassName { get; set; }
    }
}
