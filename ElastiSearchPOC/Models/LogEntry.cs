using Nest;

namespace ElastiSearchPOC.Models
{
    public class LogEntry
    {
        public string Message { get; set; } = string.Empty;
        public string Service { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty; // Error, Warning, Info
        public string Environment { get; set; } = string.Empty; // Production, Staging

        [PropertyName("@timestamp")]
        public DateTime Timestamp { get; set; }

        public string? ExceptionStack { get; set; } // Not indexed

        [Keyword]
        public string UniqueId { get; set; } = Guid.NewGuid().ToString();
    }
}
