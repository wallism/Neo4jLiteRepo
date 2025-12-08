namespace Neo4jLiteRepo.Models;

public class SequenceText
{
    public string Text { get; set; }
    public int Sequence { get; set; }

    public override string ToString() => $"[{Sequence}] {Text}";

    public static SequenceText Build(string replaceHtmlEntities, int sequence) =>
        new()
        {
            Text = replaceHtmlEntities,
            Sequence = sequence
        };

}