namespace Neo4jLiteRepo.Helpers
{
    public static class DataLoadHelpers
    {
        public static async Task<string> LoadJsonFromFile(string fullFilePath)
        {
            var fileName = Path.GetFileName(fullFilePath);

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"read data from file {fileName}");
            Console.ResetColor();

            if (!File.Exists(fullFilePath))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"file {fileName} does not exist");
                Console.ResetColor();
                return string.Empty;
            }

            var json = await File.ReadAllTextAsync(fullFilePath);
            return json;
        }

        public static string GetFullFilePath<T>(string sourceFilesRootPath, string? fileName = null, string extension = ".json") where T : GraphNode
        {
            var path = $"{Path.Combine(sourceFilesRootPath, fileName ?? typeof(T).Name)}";
            // If the path doesn't have an extension, add .json
            if (string.IsNullOrEmpty(Path.GetExtension(path)))
            {
                path = Path.ChangeExtension(path, extension);
            }
            return path;
        }
    }
}
