
using Neo4jLiteRepo.Models;

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
        /// Converts a value to nullable DateTimeOffset. Handles DateTime, strings and common temporal-like objects.
        /// Returns null if the value is null or conversion fails.
        /// </summary>
        public static DateTimeOffset? ConvertToNullableDateTimeOffset(this object? value)
        {
            if (value is null)
            {
                return null;
            }

            var result = ConvertToDateTimeOffset(value);
            return result == DateTimeOffset.MinValue ? null : result;
        }

        /// <summary>
        /// Converts a value to nullable DateTime. Handles DateTimeOffset and string.
        /// Returns null if the value is null or conversion fails.
        /// </summary>
        public static DateTime? ConvertToNullableDateTime(this object? value)
        {
            if (value is null)
            {
                return null;
            }

            var result = ConvertToDateTime(value);
            return result == DateTime.MinValue ? null : result;
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

        /// <summary>
        /// Converts a raw Neo4j value (expected string) into a SequenceText instance.
        /// Since only the textual portion is persisted (sequence number discarded when writing),
        /// the Sequence is set to 0 by default. Returns a default object with empty text on failure.
        /// </summary>
        public static SequenceText ConvertToSequenceText(this object value)
        {
            try
            {
                if (value is SequenceText st)
                {
                    return st;
                }

                var text = value?.ToString() ?? string.Empty;
                return new SequenceText
                {
                    Text = text,
                    Sequence = 0
                };
            }
            catch
            {
                return new SequenceText { Text = string.Empty, Sequence = 0 };
            }
        }

        /// <summary>
        /// Converts a raw Neo4j value into a list of SequenceText. We store lists (BulletPoints, Notes)
        /// as a single flattened comma-separated string on write. On read, we split by comma and recreate
        /// ordered SequenceText entries (index-based sequence). If the raw value is already an IEnumerable
        /// we enumerate and convert each element's string representation.
        /// </summary>
        public static List<SequenceText> ConvertToSequenceTextList(this object value)
        {
            var list = new List<SequenceText>();
            try
            {
                if (value is List<SequenceText> existing)
                {
                    return existing;
                }

                IEnumerable<string> parts;
                switch (value)
                {
                    case string s:
                        // Split on comma – trimming whitespace. Empty entries removed.
                        parts = s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                        break;
                    case IEnumerable<string> strEnum:
                        parts = strEnum;
                        break;
                    case IEnumerable<object> objEnum:
                        parts = objEnum.Select(o => o?.ToString() ?? string.Empty);
                        break;
                    default:
                        parts = [value?.ToString() ?? string.Empty];
                        break;
                }

                var index = 0;
                foreach (var p in parts)
                {
                    if (string.IsNullOrWhiteSpace(p)) continue;
                    list.Add(new SequenceText { Text = p, Sequence = index++ });
                }
            }
            catch
            {
                // swallow – return what we have
            }
            return list;
        }

        /// <summary>
        /// Converts a value to an enum of type TEnum. Handles string and numeric values.
        /// Returns the default enum value (0) on failure.
        /// </summary>
        public static TEnum ConvertToEnum<TEnum>(this object value) where TEnum : struct, Enum
        {
            try
            {
                // If already the correct enum type
                if (value is TEnum enumValue)
                {
                    return enumValue;
                }

                // Handle string conversion (case-insensitive)
                if (value is string str && !string.IsNullOrWhiteSpace(str))
                {
                    if (Enum.TryParse<TEnum>(str, ignoreCase: true, out var parsed))
                    {
                        return parsed;
                    }
                }

                // Handle numeric values
                if (value is int or long or short or byte)
                {
                    return (TEnum)Enum.ToObject(typeof(TEnum), value);
                }

                // Try converting to underlying type and then to enum
                var underlyingType = Enum.GetUnderlyingType(typeof(TEnum));
                var converted = Convert.ChangeType(value, underlyingType);
                return (TEnum)Enum.ToObject(typeof(TEnum), converted);
            }
            catch
            {
                // Return default value (typically 0)
                return default;
            }
        }

        /// <summary>
        /// Converts a value to a nullable enum of type TEnum. Handles string and numeric values.
        /// Returns null if the value is null or conversion fails.
        /// </summary>
        public static TEnum? ConvertToNullableEnum<TEnum>(this object? value) where TEnum : struct, Enum
        {
            if (value is null)
            {
                return null;
            }

            try
            {
                return ConvertToEnum<TEnum>(value);
            }
            catch
            {
                return null;
            }
        }
    }
}
