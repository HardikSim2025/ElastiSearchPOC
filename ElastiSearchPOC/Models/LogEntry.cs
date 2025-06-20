namespace ElastiSearchPOC.Models
{
    public class LogEntry
    {
        public string Message { get; set; } = string.Empty;
        public string Service { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty; // Error, Warning, Info
        public string Environment { get; set; } = string.Empty; // Production, Staging
        public DateTime Timestamp { get; set; }
        public string? ExceptionStack { get; set; } // Not indexed
    }
}
