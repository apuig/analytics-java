# Client alternative for Server

This new Segment analytics client for Java is designed to address the shortcomings of both `analytics-java` and especially `analytics-kotlin`.

---

### 1. **Server is not a priority**
This project is designed specifically for server-side use cases, prioritizing reliability, configurability, and operational transparency for backend workloads. Unlike `analytics-kotlin`, which is optimized for device-mode and mobile, this implementation is server-centric from the ground up.

#### 1.1 **Always persisting to disk**
Events are only written to disk if uploads fail or queues overflow, minimizing disk I/O under normal conditions. This is a middle ground between `analytics-java` (no durability) and `analytics-kotlin` (always persists).

#### 1.2 **No HA support / Batch file overwrites**
Batch files are uniquely named and do not rely on a shared index file, making the implementation safe for multiple processes or HA deployments.

#### 1.3 **Poor retry mechanism / No circuit breaker**
We provide a configurable, robust retry mechanism with exponential backoff and a circuit breaker to avoid hammering the Segment API during outages or rate limits.

#### 1.4 **Optimized for device-mode / userId handling**
Our implementation is **stateless** regarding user identity and does not assume a single user context. The userId must be explicitly provided with each event, making it suitable for multi-user server environments.

#### 1.5 **Unbounded memory usage under disk latency**
We implement strict backpressure and bounded queues. If disk or network is slow, the client will block or reject new events, preventing unbounded memory growth.

#### 1.6 **Unfriendly shutdown (JVM shutdown-hooks, plugin classloaders)**
We avoid reliance on JVM shutdown hooks and plugin classloaders, ensuring predictable shutdown behavior in all environments, including those using custom classloaders.


---

### 2. **Unnecessary complexity**
This project does not support or require plugin architectures, avoiding the complexity and maintenance burden seen in `analytics-kotlin`. The codebase is simpler and easier to understand and maintain. It is intentionally kept simple, with clear separation of concerns and minimal dependencies, making it easy for teams familiar with `analytics-java` to adopt and contribute.

#### 2.1 **In-house concurrency frameworks (Sovran-Kotlin)**
We use standard Java concurrency primitives, avoiding custom or in-house frameworks, which simplifies debugging and maintenance.

#### 2.2 **Kotlin dependency and ecosystem/tooling friction**
This project is pure Java, avoiding any Kotlin dependencies. This improves compatibility, reduces maintenance overhead, and ensures better tooling support across all Java environments. As a pure Java implementation, there is no additional Kotlin runtime thread overhead.

#### 2.3 **Undocumented CDN settings interaction / Endpoint configuration**
The upload endpoint is fully configurable via documented settings. There is no dependency on undocumented CDN settings interaction or external configuration fetches.

---

### Summary

This project is inspired by `analytics-java`, but adds persistence for reliability. It directly addresses the pain points with both `analytics-java` and especially `analytics-kotlin`, providing a robust and maintainable alternative for Java server environments.
