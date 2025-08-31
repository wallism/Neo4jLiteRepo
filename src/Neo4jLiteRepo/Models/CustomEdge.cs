namespace Neo4jLiteRepo.Models;

/// <summary>
/// If the edge has custom properties, create a class that inherits from this.
/// </summary>
public abstract class CustomEdge
{
    public override string ToString() => $"[{GetType().Name}] from:{GetFromId()} to:{GetToId()}";

    public abstract string GetFromId();
    public abstract string GetToId();
}

[AttributeUsage(AttributeTargets.Property)]
public class EdgePropertyIgnoreAttribute : Attribute
{
}