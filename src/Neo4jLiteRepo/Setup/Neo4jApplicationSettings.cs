namespace Neo4jLiteRepo.Setup
{
    public class Neo4jSettings
    {
        public Uri? ConnectionUri { get; set; }

        public required string User { get; set; }

        public required string Password { get; set; }

        public required string Database { get; set; }
    }
}
