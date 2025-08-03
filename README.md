# Reliability Overview

This implementation provides a middle ground between `analytics-java` (not durable, may lose events) and `analytics-kotlin` (writes every message to disk before upload).

**Key reliability features:**
- Messages are written to disk only if upload fails or if too many messages are waiting to be uploaded.
- Configurable, durable retry mechanism for failed uploads.
- Circuit breaker for HTTP uploads to prevent repeated failures.

## Reliability Tradeoffs During Ungraceful Shutdown

- **Possible Event Loss:** Events may be lost if the process crashes while messages are:
  - Waiting in memory to be uploaded (`http.size`)
  - Waiting to be batched (`http.flushSize`, `http.flushMs`)
  - In-flight in HTTP requests (`http.executorSize`)
- For typical loads, events created within `http.flushMs` (default 10s) before a crash may be lost.

## Back Pressure & Edge Cases

- If Segment API latency is high and concurrent uploads are limited, back pressure moves upload execution to internal threads.
- If file system latency is also high, overflow queues block the calling code until batches can be written to disk.

## Architecture Overview

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
