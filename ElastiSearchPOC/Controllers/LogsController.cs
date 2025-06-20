using ElastiSearchPOC.Models;
using ElastiSearchPOC.Services;
using Microsoft.AspNetCore.Mvc;

namespace ElastiSearchPOC.Controllers
{
    [ApiController]
    [Route("api/logs")]
    public class LogsController : ControllerBase
    {
        private readonly ElasticsearchService _esService;

        public LogsController(ElasticsearchService esService)
        {
            _esService = esService;
        }

        [HttpPost("search/with-filter")]
        public async Task<ActionResult<LogQueryResponse>> SearchWithFilter([FromBody] LogQueryRequest request)
        {
            var result = await _esService.SearchLogsUsingFilterAsync(request);
            return Ok(result);
        }

        [HttpPost("search/without-filter")]
        public async Task<ActionResult<LogQueryResponse>> SearchWithoutFilter([FromBody] LogQueryRequest request)
        {
            var result = await _esService.SearchLogsUsingMustOnlyAsync(request);
            return Ok(result);
        }

        [HttpPut("add")]
        public async Task<IActionResult> AddLog([FromBody] LogEntry log)
        {
            log.Timestamp = log.Timestamp == default ? DateTime.UtcNow : log.Timestamp;
            await _esService.IndexLogAsync(log);
            return Accepted();
        }
    }
}
