# Why

Provide a different reliability tradeoff 
- `analytics-java`: is not durable at all, it can lose events if you don't implement the appropriate callbacks and retries
- `analytics-kotlin`: on the opposite direction, writes every single message to disk before attempt to upload it

This implementation only write to disk when
- upload fail
- too many messages waiting to be uploaded 

It also provides
- configurable and durable retry mechanism
- circuit breaker on the HTTP upload


# Edge cases

### Ungraceful shutdown
**It can lose events**
- `http.size` messages waiting to be uploaded
- `http.flushSize` messages waiting to create a batch
- `http.flushMs` messages waiting to create a batch
- `http.executorSize` in flight HTTP requests

For a regular load it means it can lose events created `http.flushMs` (default 10s) before the crash

> Please note `analytics-kotlin` also implements some flush mechanism, meaning it could also lost events

### Segment API latency is high and the configured number of concurrent uploads cannot meet the request rate
The back pressure will **move the upload execution** (CallerRun Policy on the networkExecutor) to this threads
- Upload BatchQueue: meaning the consumption of pending to upload messages will be delayed
- RetryUpload: meaning it will not retry more files until the upload pool have some slot 

#### ... and file system latency is also high
In this case we can overflow both BatchQueue.
The overflow BatchQueue use `put`, so it will **block the calling code**, waiting to write some batch to disk


# Overview

```mermaid
sequenceDiagram
    box ClientCall
        participant SegmentService
    end
    participant SegmentClient
    box HTTP
        participant QueueUpload        
        participant Upload
    end
    box File
        participant QueueStorage
        participant Storage
    end
    box Retry
        participant Retry
    end

    activate SegmentService
    SegmentService->>+SegmentClient: enqueue    
    SegmentClient<<->>QueueUpload: offer 
    alt overflow in QueueUpload
        SegmentClient<<->>QueueStorage: put
    end
    SegmentClient->>-SegmentService: 
    deactivate SegmentService

    loop Batch thread
        QueueUpload->>QueueUpload:poll
        note over QueueUpload: flush size<br/>flush timeout<br/>batchSize
        activate QueueUpload
    end
    QueueUpload->>+Upload: upload batch    
    note over  Upload: HTTP requests submited to a pool
    Upload->>QueueUpload: 
    deactivate QueueUpload

    alt upload failed
    note over Upload: CircuitBreaker
        Upload->>+Storage: write batch
        note over Storage: fileName with timestamp<br/>retryAfter<br/>retryCount
        Storage->>-Upload: 
    end    
    deactivate  Upload

    loop Batch thread
        QueueStorage->>QueueStorage:poll
        note over QueueStorage: flush size<br/>flush timeout<br/>batchSize
        activate QueueStorage       
    end    
    QueueStorage->>+Storage: wirte overflow batch
    
    note over Storage: fileName with timestamp<br/>retryAfter<br/>retryCount
    Storage->>-QueueStorage:     
    deactivate QueueStorage

    
    loop scheduled check        
        activate Retry
        Retry->>+Storage: list files
        note over Retry: find files with retryAfter
        Storage->>-Retry: 

        loop for each file            
            Retry->>+Upload: upload
            alt succeed
                Upload->>+Storage: delete file
                Storage->>-Upload: 
            end
            alt failed        
                Upload->>+Storage: move file to next retryAfter
                Storage->>-Upload: 
            end
            Upload->>-Retry:             
        end
        deactivate Retry
    end
```