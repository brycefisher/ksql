-- Create topic cdn-logs
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/myURI.schema.json",
  "title": "CDN log",
  "description": "Sample schema to help you get started.",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "asset_id": {
      "type": "number",
      "description": "The integer type is used for integral numbers."
    },
     "request_begin": {
      "type": "number",
      "description": "The integer type is used for integral numbers."
    },
     "segment_duration": {
      "type": "number",
      "description": "The integer type is used for integral numbers."
    },
     "status_code": {
      "type": "number",
      "description": "The integer type is used for integral numbers."
    },
     "environment_id": {
      "type": "number",
      "description": "The integer type is used for integral numbers."
    }
  }
}

-- Create a new stream
CREATE STREAM cdn_logs_stream_json
    (asset_id BIGINT, request_begin BIGINT, segment_duration BIGINT, status_code BIGINT, environment_id BIGINT)
  WITH (kafka_topic='cdn-logs-json', value_format='json', timestamp='request_begin', partitions=1);

-- Query the stream

SELECT
    asset_id,
    environment_id,
    status_code,
    sum(segment_duration) AS delivered_seconds
    FROM CDN_LOGS_STREAM_JSON
    WHERE
      status_code >= 200 AND status_code < 300
    GROUP BY
        asset_id,
        environment_id,
        status_code
    EMIT CHANGES;


-- Query with tumbling 
SELECT 

   from_unixtime(max(ROWTIME)) as Window_Emit,
   asset_id,
   sum(segment_duration) AS delivered_seconds
FROM CDN_LOGS_STREAM_JSON
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY asset_id
EMIT CHANGES;

--Create table from tumbling
CREATE TABLE ASSET_DURATION_TABLE AS
SELECT 
   from_unixtime(max(ROWTIME)) as Window_Emit,
   asset_id,
   sum(segment_duration) AS delivered_seconds
FROM CDN_LOGS_STREAM_JSON
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY asset_id
EMIT CHANGES;

--Create table with aggregations - will be updated as you query
CREATE TABLE ASSET_DURATION_AGG AS
SELECT 
   asset_id,
   sum(segment_duration) AS delivered_seconds
FROM CDN_LOGS_STREAM_JSON
GROUP BY asset_id;
