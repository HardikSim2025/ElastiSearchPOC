namespace ElastiSearchPOC.Models
{
    public class LogQueryRequest
    {
        public string? Keyword { get; set; }
        public string? Service { get; set; }
        public string? Category { get; set; }
        public string? Environment { get; set; }
        public DateTime? From { get; set; }
        public DateTime? To { get; set; }
    }
}
