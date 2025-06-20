using ElastiSearchPOC.Models;
using Elasticsearch.Net;
using Nest;

namespace ElastiSearchPOC.Services
{
    public class ElasticsearchService
    {
        private readonly IElasticClient _client;

        public ElasticsearchService(IElasticClient client)
        {
            _client = client;
        }

        private string GetIndexName(DateTime date) => $"logs-{date:yyyy-MM-dd}";

        public async Task IndexLogAsync(LogEntry log)
        {
            var indexName = GetIndexName(log.Timestamp);
            await _client.IndexAsync(log, idx => idx.Index(indexName));
        }

        public async Task<LogQueryResponse> SearchLogsUsingFilterAsync(LogQueryRequest request)
        {
            var searchRequest = new SearchRequest(GetIndexName(DateTime.UtcNow) + "*")
            {
                Query = new BoolQuery
                {
                    Filter = GetFilters(request)
                }
            };

            var response = await _client.SearchAsync<LogEntry>(searchRequest);
            return new LogQueryResponse
            {
                TookMilliseconds = response.Took,
                Logs = response.Documents.ToList()
            };
        }

        public async Task<LogQueryResponse> SearchLogsUsingMustOnlyAsync(LogQueryRequest request)
        {
            var searchRequest = new SearchRequest(GetIndexName(DateTime.UtcNow) + "*")
            {
                Query = new BoolQuery
                {
                    Must = GetMusts(request)
                }
            };

            var response = await _client.SearchAsync<LogEntry>(searchRequest);
            return new LogQueryResponse
            {
                TookMilliseconds = response.Took,
                Logs = response.Documents.ToList()
            };
        }

        private List<QueryContainer> GetFilters(LogQueryRequest req)
        {
            var filters = new List<QueryContainer>();
            if (!string.IsNullOrEmpty(req.Keyword))
                filters.Add(new MatchQuery { Field = "message", Query = req.Keyword });
            if (!string.IsNullOrEmpty(req.Service))
                filters.Add(new TermQuery { Field = "service.keyword", Value = req.Service });
            if (!string.IsNullOrEmpty(req.Category))
                filters.Add(new TermQuery { Field = "category.keyword", Value = req.Category });
            if (!string.IsNullOrEmpty(req.Environment))
                filters.Add(new TermQuery { Field = "environment.keyword", Value = req.Environment });
            if (req.From.HasValue || req.To.HasValue)
                filters.Add(new DateRangeQuery
                {
                    Field = "timestamp",
                    GreaterThanOrEqualTo = req.From,
                    LessThanOrEqualTo = req.To
                });
            return filters;
        }

        private List<QueryContainer> GetMusts(LogQueryRequest req)
        {
            var musts = new List<QueryContainer>();
            if (!string.IsNullOrEmpty(req.Keyword))
                musts.Add(new MatchQuery { Field = "message", Query = req.Keyword });
            if (!string.IsNullOrEmpty(req.Service))
                musts.Add(new MatchQuery { Field = "service", Query = req.Service });
            if (!string.IsNullOrEmpty(req.Category))
                musts.Add(new MatchQuery { Field = "category", Query = req.Category });
            if (!string.IsNullOrEmpty(req.Environment))
                musts.Add(new MatchQuery { Field = "environment", Query = req.Environment });
            if (req.From.HasValue || req.To.HasValue)
                musts.Add(new DateRangeQuery
                {
                    Field = "timestamp",
                    GreaterThanOrEqualTo = req.From,
                    LessThanOrEqualTo = req.To
                });
            return musts;
        }
    }
}
