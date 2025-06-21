using ElastiSearchPOC.Models;
using ElastiSearchPOC.Services;
using Microsoft.AspNetCore.Mvc;

namespace ElastiSearchPOC.Controllers
{
    [ApiController]
    [Route("api/logs")]
    public class LogsController : ControllerBase
    {
        private readonly ElasticsearchService _elasticsearchService;

        public LogsController(ElasticsearchService elasticsearchService)
        {
            _elasticsearchService = elasticsearchService;
        }

        [HttpPost("search/with-filter")]
        public async Task<ActionResult<LogQueryResponse>> SearchWithFilter([FromBody] LogQueryRequest request)
        {
            var result = await _elasticsearchService.SearchLogsUsingFilterAsync(request);
            return Ok(new List<long>{ result.TookMilliseconds, result.Logs.Count});
        }

        [HttpPost("search/without-filter")]
        public async Task<ActionResult<LogQueryResponse>> SearchWithoutFilter([FromBody] LogQueryRequest request)
        {
            var result = await _elasticsearchService.SearchLogsUsingMustOnlyAsync(request);
            return Ok(new List<long> { result.TookMilliseconds, result.Logs.Count });
        }

        [HttpPut("add")]
        public async Task<IActionResult> AddLog([FromBody] LogEntry log)
        {
            log.Timestamp = log.Timestamp == default ? DateTime.UtcNow : log.Timestamp;
            await _elasticsearchService.IndexLogAsync(log);
            return Accepted();
        }

        [HttpPost("generate-random-logs")]
        public async Task<IActionResult> GenerateRandomLogs()
        {
            int inserted = await _elasticsearchService.GenerateAndInsertRandomLogsAsync();
            return Ok(new { Inserted = inserted });
        }

        [HttpPost("search-after")]
        public async Task<IActionResult> SearchAfter([FromBody] SearchAfterRequest request)
        {
            var (logs, nextSearchAfter) = await _elasticsearchService.SearchLogsWithSearchAfterAsync(
                request.Query, request.PageSize, request.SearchAfter);

            return Ok(new
            {
                Logs = logs,
                NextSearchAfter = nextSearchAfter
            });
        }
    }

    public class SearchAfterRequest
    {
        public LogQueryRequest Query { get; set; } = new();
        public int PageSize { get; set; } = 100;
        public object[]? SearchAfter { get; set; }
    }
}
