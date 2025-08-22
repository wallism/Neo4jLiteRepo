
namespace Neo4jLiteRepo.Helpers
{
    /// <summary>
    /// Conversion helpers for mapping Neo4j values to .NET types.
    /// Implemented as extension methods on object for convenient use in expression trees.
    /// </summary>
    public static class ValueConversionExtensions
    {
        /// <summary>
        /// Converts a value to a float array, supporting float[], double[] and IEnumerable.
        /// Returns an empty array on failure.
        /// </summary>
        public static float[] ConvertToFloatArray(this object value)
        {
            if (value is float[] f)
            {
                return f;
            }

            if (value is double[] d)
            {
                return d.Select(x => (float)x).ToArray();
            }

            if (value is IEnumerable<object> objEnum)
            {
                return objEnum.Select(Convert.ToSingle).ToArray();
            }

            return [];
        }

        /// <summary>
        /// Converts a value to Guid, supporting Guid and string.
        /// Returns Guid.Empty on failure.
        /// </summary>
        public static Guid ConvertToGuid(this object value)
        {
            if (value is Guid g)
            {
                return g;
            }

            if (value is string s && Guid.TryParse(s, out var parsed))
            {
                return parsed;
            }

            return Guid.Empty;
        }

        /// <summary>
        /// Converts a value to DateTimeOffset. Handles DateTime, strings and common temporal-like objects.
        /// Returns DateTimeOffset.MinValue on failure.
        /// </summary>
        public static DateTimeOffset ConvertToDateTimeOffset(this object value)
        {
            if (value is DateTimeOffset dto)
            {
                return dto;
            }

            if (value is DateTime dt)
            {
                if (dt.Kind == DateTimeKind.Unspecified)
                {
                    dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                }

                return new DateTimeOffset(dt.ToUniversalTime());
            }

            string? candidate = value as string;

            var type = value.GetType();
            if (candidate is null && type.Name.Contains("Date", StringComparison.OrdinalIgnoreCase) && type.Name.Contains("Time", StringComparison.OrdinalIgnoreCase))
            {
                candidate = value.ToString();
            }

            if (!string.IsNullOrWhiteSpace(candidate))
            {
                if (DateTimeOffset.TryParse(candidate, out var parsedFromString))
                {
                    return parsedFromString;
                }

                if (DateTime.TryParse(candidate, out var parsedDt))
                {
                    return new DateTimeOffset(DateTime.SpecifyKind(parsedDt, DateTimeKind.Utc));
                }
            }

            try
            {
                var yearProp = type.GetProperty("Year");
                var monthProp = type.GetProperty("Month");
                var dayProp = type.GetProperty("Day");
                var hourProp = type.GetProperty("Hour");
                var minuteProp = type.GetProperty("Minute");
                var secondProp = type.GetProperty("Second");
                if (yearProp != null && monthProp != null && dayProp != null && hourProp != null && minuteProp != null && secondProp != null)
                {
                    int year = Convert.ToInt32(yearProp.GetValue(value));
                    int month = Convert.ToInt32(monthProp.GetValue(value));
                    int day = Convert.ToInt32(dayProp.GetValue(value));
                    int hour = Convert.ToInt32(hourProp.GetValue(value));
                    int minute = Convert.ToInt32(minuteProp.GetValue(value));
                    int second = Convert.ToInt32(secondProp.GetValue(value));
                    long nanos = 0;
                    var nanoProp = type.GetProperty("Nanosecond") ?? type.GetProperty("Nanoseconds");
                    if (nanoProp?.GetValue(value) is { } nanoVal)
                    {
                        nanos = Convert.ToInt64(nanoVal);
                    }

                    var baseDt = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc);
                    if (nanos > 0)
                    {
                        baseDt = baseDt.AddTicks(nanos / 100);
                    }

                    var offsetSecondsProp = type.GetProperty("OffsetSeconds") ?? type.GetProperty("ZoneOffsetSeconds");
                    TimeSpan offset = TimeSpan.Zero;
                    if (offsetSecondsProp?.GetValue(value) is { } offVal)
                    {
                        offset = TimeSpan.FromSeconds(Convert.ToInt32(offVal));
                    }

                    return new DateTimeOffset(baseDt, offset).ToUniversalTime();
                }
            }
            catch
            {
            }

            return DateTimeOffset.MinValue;
        }

        /// <summary>
        /// Converts a value to DateTime. Handles DateTimeOffset and string.
        /// Returns DateTime.MinValue on failure.
        /// </summary>
        public static DateTime ConvertToDateTime(this object value)
        {
            if (value is DateTime dt)
            {
                return dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt;
            }

            if (value is DateTimeOffset dto)
            {
                return dto.UtcDateTime;
            }

            if (value is string s && DateTime.TryParse(s, out var parsed))
            {
                if (parsed.Kind == DateTimeKind.Unspecified)
                {
                    parsed = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
                }

                return parsed;
            }

            return DateTime.MinValue;
        }

        /// <summary>
        /// Converts a value to a List of string. Handles IEnumerable cases and scalar string.
        /// Returns empty list on failure.
        /// </summary>
        public static List<string> ConvertToStringList(this object value)
        {
            if (value is List<string> ls)
            {
                return ls;
            }

            if (value is string str)
            {
                return new List<string> { str };
            }

            if (value is IEnumerable<object> objs)
            {
                return objs.Select(o => o?.ToString() ?? string.Empty).ToList();
            }

            if (value is IEnumerable<string> strEnum)
            {
                return strEnum.ToList();
            }

            return [];
        }
    }
}
