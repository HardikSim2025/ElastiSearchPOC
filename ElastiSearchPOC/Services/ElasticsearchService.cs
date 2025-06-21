using ElastiSearchPOC.Models;
using Elasticsearch.Net;
using Nest;
using System.Text.Json;

namespace ElastiSearchPOC.Services
{
    public class ElasticsearchService
    {
        private readonly IElasticClient _client;

        public ElasticsearchService(IElasticClient client)
        {
            _client = client;
        }

        // Change this method to always return the data stream name
        private string GetIndexName(DateTime date) => "logs";

        // Update IndexLogAsync to use the data stream name
        public async Task IndexLogAsync(LogEntry log)
        {
            await _client.IndexAsync(log, idx => idx.Index(GetIndexName(log.Timestamp)));
        }

        // Update Search methods to use the data stream name
        public async Task<LogQueryResponse> SearchLogsUsingFilterAsync(LogQueryRequest request)
        {
            var searchRequest = new SearchRequest("logs")
            {
                Size = 10000, // Set to desired number
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
            var searchRequest = new SearchRequest("logs")
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

        public async Task<(List<LogEntry> Logs, object[]? NextSearchAfter)> SearchLogsWithSearchAfterAsync(
            LogQueryRequest request, int pageSize, object[]? searchAfter = null)
        {
            var searchRequest = new SearchRequest("logs")
            {
                Size = pageSize,
                Query = new BoolQuery
                {
                    Filter = GetFilters(request)
                },
                Sort = new List<ISort>
                {
                    new FieldSort { Field = "@timestamp", Order = SortOrder.Descending },
                    new FieldSort { Field = "uniqueId", Order = SortOrder.Ascending }
                }
            };

            if (searchAfter != null)
            {
                searchRequest.SearchAfter = searchAfter
                .Select(val =>
                {
                    if (val is JsonElement je)
                    {
                        return je.ValueKind switch
                        {
                            _ => je.ToString()
                        };
                    }
                    return val;
                })
                .ToArray();
            }

            var response = await _client.SearchAsync<LogEntry>(searchRequest);

            var logs = response.Documents.ToList();
            object[]? nextSearchAfter = null;
            if (logs.Count > 0 && response.HitsMetadata?.Hits?.LastOrDefault() != null)
                nextSearchAfter = response.HitsMetadata.Hits.Last().Sorts?.ToArray(); // Explicit conversion to object[]

            if (nextSearchAfter != null && nextSearchAfter.Length > 0 && nextSearchAfter[0] is DateTime timestamp)
            {
                nextSearchAfter[0] = timestamp.ToString("o"); ;
            }

            return (logs, nextSearchAfter);
        }

        private List<QueryContainer> GetFilters(LogQueryRequest req)
        {
            var filters = new List<QueryContainer>();
            if (!string.IsNullOrEmpty(req.Keyword))
                filters.Add(new MatchQuery { Field = "message", Query = req.Keyword });

            // adding below fields in filters as term for optimization which are in index as keyword
            if (!string.IsNullOrEmpty(req.Service))
                filters.Add(new TermQuery { Field = "service.keyword", Value = req.Service });
            if (!string.IsNullOrEmpty(req.Category))
                filters.Add(new TermQuery { Field = "category.keyword", Value = req.Category });
            if (!string.IsNullOrEmpty(req.Environment))
                filters.Add(new TermQuery { Field = "environment.keyword", Value = req.Environment });

            // adding below date range fields in filters as range for optimization
            if (req.From.HasValue || req.To.HasValue)
                filters.Add(new DateRangeQuery
                {
                    Field = "@timestamp",
                    GreaterThanOrEqualTo = req.From,
                    LessThanOrEqualTo = req.To
                });

            return filters;
        }

        private List<QueryContainer> GetMusts(LogQueryRequest req)
        {
            // this query is not optimized as we are adding all fields as match query
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

        public async Task CreateLogsIndexTemplateAsync()
        {
            var templateName = "logs_template_v2";
            var indexPattern = "logs-*";
            // we install elasticsearch on localhost:9200, so we need to delete the template which is added by default
            var deleteTemplateResponse = await _client.Indices.DeleteTemplateV2Async("logs");

            // Check if the composable template already exists
            var existsResponse = await _client.Indices.TemplateV2ExistsAsync(templateName);
            if (existsResponse.Exists)
            {
                var getDataStreamResponse = await _client.Indices.GetDataStreamAsync("logs");
                if(!getDataStreamResponse.DataStreams.Any())
                {
                    var createDataStreamResponseR = await _client.Indices.CreateDataStreamAsync("logs");
                    if (!createDataStreamResponseR.IsValid)
                    {
                        throw new Exception($"Failed to create data stream: {createDataStreamResponseR.ServerError}");
                    }
                }
                return;
            }

            var putTemplateResponse = await _client.Indices.PutTemplateV2Async(templateName, t => t
                .IndexPatterns(indexPattern, "logs")
                .DataStream(new DataStream())
                .Template(temp => temp
                    .Settings(s => s
                        .NumberOfShards(1)
                        .NumberOfReplicas(1)
                    )
                    .Mappings(m => m
                        .Properties(ps => ps
                            .Text(s => s.Name("message"))
                            .Keyword(s => s.Name("service"))
                            .Keyword(s => s.Name("category"))
                            .Keyword(s => s.Name("environment"))
                            .Date(s => s.Name("@timestamp")) // Required timestamp field for data streams
                            .Object<object>(o => o
                                .Name("exception_stack")
                                .Enabled(false)
                            )
                            .Keyword(s => s.Name("uniqueId")) // Add uniqueId field
                        )
                    )
                )
                .Priority(200) 
            );

            if (!putTemplateResponse.IsValid)
            {
                throw new Exception($"Failed to create composable index template: {putTemplateResponse.ServerError}");
            }

            var createDataStreamResponse = await _client.Indices.CreateDataStreamAsync("logs");
            if (!createDataStreamResponse.IsValid)
            {
                throw new Exception($"Failed to create data stream: {createDataStreamResponse.ServerError}");
            }

        }

        // Update BulkIndexLogsAsync to handle large payloads
        private async Task BulkIndexLogsAsync(IEnumerable<LogEntry> logs)
        {
            var bulkRequest = new BulkRequest()
            {
                Refresh = Refresh.WaitFor,
                Operations = new List<IBulkOperation>(), // Ensure the data is searchable immediately after indexing,
            };

            foreach (var log in logs)
            {
                bulkRequest.Operations.Add(new BulkCreateOperation<LogEntry>(log)
                {
                    Index = GetIndexName(log.Timestamp)
                });
            }

            var bulkResponse = await _client.BulkAsync(bulkRequest);

            if (bulkResponse.ItemsWithErrors.Where(x => x.Error != null).Count() > 0)
            {
                var errors = string.Join("\n", bulkResponse.ItemsWithErrors.Where(x => x.Error != null).Select(i =>
                    $"Index: {i.Index}, Error: {i.Error?.Reason}"));
                throw new Exception($"Bulk insert had errors:\n{errors}");
            }
        }

        public async Task<int> GenerateAndInsertRandomLogsAsync(int count = 1000000)
        {
            var categories = new[] { "Info", "Debug", "ErrorDebug", "Error", "Warning" };
            var environments = new[] { "Dev", "Qa", "Production" };
            var services = new[] { "OrderService", "UserService", "PaymentService", "InventoryService" };
            var random = new Random();
            var totalInserted = 0;
            const int batchSize = 10000; // Process 10k records at a time

            for (int batch = 0; Math.Ceiling(count / (double)batchSize) > batch; batch++)
            {
                var currentBatchSize = Math.Min(batchSize, count - (batch * batchSize));
                var logs = new List<LogEntry>(currentBatchSize);

                for (int i = 0; i < currentBatchSize; i++)
                {
                    var category = categories[random.Next(categories.Length)];
                    var environment = environments[random.Next(environments.Length)];
                    var service = services[random.Next(services.Length)];
                    var message = $"Log message {totalInserted + i + 1} - {category} - {environment} - {service} - {DateTime.UtcNow.ToString("yyyy-MM-dd")}";
                    string? exceptionStack = null;

                    if (category == "ErrorDebug" && random.NextDouble() < 0.5)
                    {
                        exceptionStack = $"System.Exception: Simulated exception for log {totalInserted + i + 1}\n   at {service}.DoWork()  - {DateTime.UtcNow.ToString("yyyy-MM-dd")}";
                    }

                    logs.Add(new LogEntry
                    {
                        Message = message,
                        Service = service,
                        Category = category,
                        Environment = environment,
                        Timestamp = DateTime.UtcNow.AddSeconds(-random.Next(0, 2*86400)),
                        ExceptionStack = exceptionStack
                    });
                }

                await BulkIndexLogsAsync(logs);
                totalInserted += logs.Count;
            }

            return totalInserted;
        }
    }
}
