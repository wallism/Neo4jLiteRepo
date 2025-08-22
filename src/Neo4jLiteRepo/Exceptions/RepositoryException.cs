namespace Neo4jLiteRepo.Exceptions
{
    /// <summary>
    /// Represents errors that occur within the repository layer (Neo4jGenericRepo).
    /// Wraps the original exception while preserving context such as the Cypher query
    /// (structure only) and parameter keys to aid diagnostics without leaking sensitive values.
    /// </summary>
    [Serializable]
    public class RepositoryException : Exception
    {
        /// <summary>
        /// The (sanitized) Cypher query text associated with the failure.
        /// </summary>
        public string? Query { get; }

        /// <summary>
        /// The parameter keys passed with the query (values intentionally omitted).
        /// </summary>
        public IReadOnlyCollection<string> ParameterKeys { get; } = Array.Empty<string>();

        public RepositoryException() { }

        public RepositoryException(string message) : base(message) { }

        public RepositoryException(string message, Exception? innerException) : base(message, innerException) { }

        public RepositoryException(string message, string? query, IEnumerable<string>? parameterKeys, Exception? innerException)
            : base(message, innerException)
        {
            Query = query;
            if (parameterKeys is not null)
                ParameterKeys = parameterKeys.Distinct().ToArray();
        }

        
        public override string ToString()
        {
            return $"{base.ToString()} | QueryLength={(Query?.Length ?? 0)} | ParamKeys=[{string.Join(",", ParameterKeys)}]";
        }
    }
}
