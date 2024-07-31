Real Time Distributed Edge Stream Processing Framework

Use cases: Real time anomaly detection, online learning, model deployments

Plans for the project:
In the application layer the user will register all nodes in the network and sources and sinks and data stream formats and node capabilities, and define pipelines and complex event patterns over the streams of data from edge device sources and sinks and their priorities and latency requirements. Sources and sinks may be anywhere on the network or an external entity such as a database, kafka topic, etc. At compile time, the distribution layer will optimize and compile the pipeline and distribute it to the nodes, creating partitions based on data locality, network topology, user-defined latency requirements and node capabilities. The distribution layer then sets up a distributed write-ahead log receiving eventually consistent updates from nodes of their activity and processing results. 

At runtime, the node runtime layer will take the information provided initially and begin performing their assigned operations on the streams from their defined input sources and sending outputs to the next stage in the pipeline. Each node and partition will have a local and distributed shared state. They will individually monitor their loads and performance and periodically send heartbeats to other nodes in their partition as well as the distribution layer with their metadata using a gossip protocol. If a node becomes overloaded, it will send a request directly to the node with the lowest load it is aware of (based on the heartbeats it received most recently) to shed its load. The distribution layer will hear about these updates and perform more complex reconfiguration if it determines the peer to peer load balancing method to be inoptimal.



Application Layer
- Node Registry: Table of all nodes and their defined capabilities, includes source nodes and stream formats
- Pipeline Definition: Methods for defining pipelines and complex event patterns
- Pipeline Manager: Register of all pipelines, latency requirements, priorities

Distribution Layer (Network Layer)
- Topology Manager (Central Metrics Collector): Detects failures
- Distribution Planner: Optimizes, compiles, and executes pipelines from pipeline manager, creates partitions based on node registry and topology manager, sends instructions to node runtimes, sets up WAL, executes rebalances from central load rebalancer 
- Central Load Rebalancer: References topology manager to detect and correct inefficient p2p actions
- Write-Ahead Log: Log of activity and processing results
- Consensus Mechanism: Determines correctness of WAL compared to information from topology manager and distributed mechanisms

Node Runtime
- Local Message Queue: Accepts messages and sends to stream processor, load balancer acceptor, state store or metrics collector depending on type. Overflow is stored in local state store
- Stream Processor: Performs defined operations on defined streams and sends results to next stage. Multi-threaded
- Local Time/State Manager: Windowed operations
- Distributed State Store (Partition-wide): Stores intermediates for stateful computations between nodes as well as heartbeats/partition registry
- Local Load Balancer
- Local Metrics Collector
- Heartbeat Gossip Protocol 
