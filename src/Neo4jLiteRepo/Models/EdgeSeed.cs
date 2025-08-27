namespace Neo4jLiteRepo.Models;

public abstract class EdgeSeed
{
    public override string ToString() => $"[{GetType().Name}] from:{FromId} to:{ToId}";

    public abstract string FromId { get; }
    public abstract string ToId { get; }
}

[AttributeUsage(AttributeTargets.Property)]
public class EdgePropertyIgnoreAttribute : Attribute
{
}