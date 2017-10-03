Command Line Interface -> Master API
=============
## Command line interface functionality
- Start new map reduce operation and wait for the response.
- Check the status and get the result of a started map reduce operation.
- Check cluster status. Is another map reduce operation currently running?

## Protobuf API. 
```
service MapReduceClient {
    // Used for uploading the payload to the cluster.
    rpc UploadPayload(stream PayloadChunk) returns (EmptyMessage) {}
    // Performs the map reduce using a payload already uploaded to the cluster.
    rpc PerformMapReduce (MapReduceRequest) returns (stream MapReduceResponse) {}
    // Get the status and result of a started map reduce operation.
    rpc MapReduceStatus (MapReduceStatusRequest) return (stream MapReduceStatusResponse)
    // Gets the status of the cluster. Used to check if it is in use or not.
    rpc ClusterStatus (EmptyMessage) returns (ClusterStatusResponse) {}
}

message PayloadChunk {
    string map_reduce_id = 1; // required
    string file_name = 2;     // required
    bytes data = 3;           // required
    string file_hash = 4;     // optional
    bool done = 5;            // optional
}

message EmptyMessage {}

message MapReduceRequest {
    string map_reduce_id = 1; // required
    bool await_response = 2;  // required
}

message MapReduceResponse {
    bytes result_data = 1; // required
    bool done = 2;     // optional
}

message ClusterStatusResponse {
    int64 queue_size = 1; // required
}

message MapReduceStatusRequest {
    string map_reduce_id = 1; // required
    bool download_result = 2; // required
}

message MapReduceStatusResponse {
    bool map_reduce_complete = 1;  // required
    string file_name = 2;          // required
    bytes result_data = 3;         // optional
    bool done = 4;                 // optional
}
```
