namespace Neo4jLiteRepo.Setup
{
    public class Neo4jSettings
    {
        public Uri? ConnectionUri { get; set; }

        public required string User { get; set; }

        public required string Password { get; set; }

        public required string Database { get; set; }

        public int ConnectionTimeoutSeconds { get; set; } = 5;

        public int ConnectionAcquisitionTimeoutSeconds { get; set; } = 5;

        public int MaxConnectionPoolSize { get; set; } = 100;

        public int MaxConnectionLifetimeMinutes { get; set; } = 30;

        public int ConnectionIdleTimeoutMinutes { get; set; } = 15;

        public int ConnectionLivenessCheckSeconds { get; set; } = 30;

        public int TransactionTimeoutSeconds { get; set; } = 120;
    }
}
