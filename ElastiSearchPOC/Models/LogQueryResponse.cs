namespace ElastiSearchPOC.Models
{
    public class LogQueryResponse
    {
        public long TookMilliseconds { get; set; }
        public List<LogEntry> Logs { get; set; } = new();
    }
}
