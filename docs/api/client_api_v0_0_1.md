Command Line Interface -> Master API
=============
## Command line interface functionality
- Start new map reduce operation and wait for the map reduce to be completed.
- Check the status of a started map reduce operation.
- Check cluster status. Is another map reduce operation currently running?

## Protobuf API. 
```
service MapReduce {
    // Used for uploading the payload to the cluster.
    rpc UploadPayload(stream PayloadChunk) returns (EmptyMessage) {}
    // Performs the map reduce using a payload already uploaded to the cluster.
    rpc PerformMapReduce (MapReduceRequest) returns (EmptyMessage) {}
    // Get the status and result of a started map reduce operation.
    rpc MapReduceStatus (MapReduceStatusRequest) return (MapReduceStatusResponse)
    // Gets the status of the cluster. Used to check if it is in use or not.
    rpc ClusterStatus (EmptyMessage) returns (ClusterStatusResponse) {}
}

message PayloadChunk {
    string map_reduce_id = 1; // required
    string file_name = 2;     // required
    bytes data = 3;           // required
    // The file_hash must be sent with the last chunk
    string file_hash = 4;     // optional
}

message EmptyMessage {}

message MapReduceRequest {
    string map_reduce_id = 1;    // required
    string output_directory = 2; // required
    bool await_completion = 3;   // required 
}

message ClusterStatusResponse {
    int64 queue_size = 1;   // required
    int64 cluster_size = 2; // required
}

message MapReduceStatusRequest {
    string map_reduce_id = 1; // required
}

message MapReduceStatusResponse {
    bool complete = 1;           // required
    string output_directory = 2; // optional 
}
```
