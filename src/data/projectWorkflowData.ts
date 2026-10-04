export interface WorkflowStage {
  id: string;
  stepNumber: string;
  title: string;
  subtitle: string;
  category: 'Ingestion' | 'Processing' | 'Storage' | 'Serving' | 'Defense';
  summary: string;
  keyComponents: string[];
  techStack: string[];
  technicalDetails: {
    heading: string;
    content: string;
    codeSnippet?: {
      language: string;
      title: string;
      code: string;
    };
    diagramFlow?: string[];
  }[];
}

export const WORKFLOW_STAGES: WorkflowStage[] = [
  {
    id: 'stage-1-problem-architecture',
    stepNumber: '01',
    title: 'Problem Formulation & End-to-End Architecture',
    subtitle: 'Threat modeling, cost-per-click incentives, and distributed streaming pipeline topology',
    category: 'Ingestion',
    summary: 'Online advertisement fraud drains over $100B annually. Fraudsters exploit Pay-Per-Click (CPC) and Cost-Per-Install (CPI) payout models via automated botnets, click injection malware, and click spamming. A batch approach (e.g. daily Hadoop/Spark jobs) fails because advertiser budgets are exhausted within minutes. Our solution implements real-time stream processing with sub-100ms detection latency using Apache Flink.',
    keyComponents: [
      'Cost-Per-Click (CPC) & Cost-Per-Install (CPI) Attribution Vulnerabilities',
      'Millisecond Latency Requirement vs Batch Processing Latency',
      'End-to-End Distributed Data Flow: Edge -> Kafka -> Flink -> RocksDB -> ClickHouse -> Dashboard',
      'Exactly-Once Processing Guarantees via Two-Phase Commit'
    ],
    techStack: ['Apache Kafka', 'Apache Flink', 'RocksDB', 'ClickHouse', 'Grafana', 'Docker'],
    technicalDetails: [
      {
        heading: '1. Why Real-Time Stream Processing is Mandatory',
        content: 'Traditional anti-fraud solutions run batch reconciliation jobs every 24 hours. By the time yesterday\'s clicks are flagged as fraudulent, the advertiser\'s daily budget has already been exhausted, legitimate campaigns have paused, and fraudulent publishers have already initiated payout withdrawal. Stream processing evaluates every click in-flight within 20-50 milliseconds before billing events trigger and attribution graphs are finalized.'
      },
      {
        heading: '2. High-Level Architecture Topology',
        content: 'The end-to-end pipeline consists of four distinct tiers:\n1. Edge Ingestion Tier: Distributed NGINX/Go click collectors receive HTTPS click beacons and forward events asynchronously into Kafka.\n2. Message Streaming Tier: Multi-partition Apache Kafka cluster with topics: raw-ad-clicks, ad-impressions, and conversion-postbacks.\n3. Stateful Stream Processing Tier: Apache Flink cluster executing event-time watermarking, keyed sliding windows, dual-stream interval joins, and CEP state machines backed by RocksDB.\n4. Sink & Serving Tier: Legitimate clicks sink to real-time billing; fraudulent clicks route to a dead-letter queue (DLQ) and ClickHouse OLAP engine for forensic dashboard visualization.',
        diagramFlow: [
          'Client Browser / Mobile App',
          'Edge Click Gateway (HTTP/2)',
          'Apache Kafka (raw-ad-clicks topic)',
          'Apache Flink (Watermarking & KeyBy IP/Publisher)',
          'RocksDB Stateful Window & CEP Evaluation',
          'Kafka Sink (clean-clicks) & DLQ (fraud-alerts)',
          'ClickHouse OLAP & Grafana Monitoring UI'
        ]
      }
    ]
  },
  {
    id: 'stage-2-data-ingestion',
    stepNumber: '02',
    title: 'High-Throughput Data Ingestion Layer',
    subtitle: 'Event beacons, Kafka partitioning strategy, Avro serialization, and event-time watermarking',
    category: 'Ingestion',
    summary: 'Every ad impression, banner click, and app install event produces a structured JSON/Avro payload containing publisher details, hardware fingerprints, network identifiers, and precision timestamps. Kafka provides horizontally scalable, fault-tolerant message queuing handling >50,000 events/second.',
    keyComponents: [
      'Click Beacon Payload Schema (Avro / JSON Schema)',
      'Kafka Partition Keying Strategy (Avoiding hot partition hotspots)',
      'Producer Settings (compression=lz4, acks=all, enable.idempotence=true)',
      'Watermark Strategy for Late Arriving & Out-of-Order Network Events'
    ],
    techStack: ['Apache Kafka 3.6', 'Confluent Schema Registry', 'Apache Avro', 'Python / Java Kafka Producer'],
    technicalDetails: [
      {
        heading: '1. Click Event Data Schema Design',
        content: 'Each click event captures behavioral, network, and attribution telemetry. Strict Avro schemas ensure backward-compatibility and efficient binary packing.',
        codeSnippet: {
          language: 'json',
          title: 'click_event_schema.avsc',
          code: `{
  "type": "record",
  "name": "AdClickEvent",
  "namespace": "com.fraudguard.stream",
  "fields": [
    { "name": "click_id", "type": "string" },
    { "name": "event_timestamp", "type": "long" },
    { "name": "publisher_id", "type": "string" },
    { "name": "campaign_id", "type": "string" },
    { "name": "advertiser_id", "type": "string" },
    { "name": "ip_address", "type": "string" },
    { "name": "asn", "type": "string" },
    { "name": "user_agent", "type": "string" },
    { "name": "click_coordinates", "type": {
        "type": "record",
        "name": "Coordinates",
        "fields": [
          { "name": "x", "type": "int" },
          { "name": "y", "type": "int" }
        ]
    }},
    { "name": "touch_pressure", "type": "float", "default": 1.0 },
    { "name": "impression_token", "type": ["null", "string"], "default": null },
    { "name": "cpc_bid", "type": "double" }
  ]
}`
        }
      },
      {
        heading: '2. Kafka Partitioning & Keying Strategy',
        content: 'Proper partitioning is critical in stream processing. In Flink, state is partitioned by key (KeyedStream). If we partition solely by IPAddress, a single NAT gateway or proxy could overload a single Kafka partition and Flink TaskManager. We employ a composite key: `publisher_id + ":" + ip_address.substring(0, ip_address.lastIndexOf("."))`. This ensures clicks from the same publisher subnet land on the same stream processor for localized sliding-window aggregation while distributing cluster load uniformly.'
      }
    ]
  },
  {
    id: 'stage-3-flink-processing',
    stepNumber: '03',
    title: 'Stream Processing with Apache Flink',
    subtitle: 'Stateful stream analytics, event-time watermarks, sliding windows, and Complex Event Processing (CEP)',
    category: 'Processing',
    summary: 'Apache Flink is the computational core. Unlike Spark Streaming (which relies on micro-batches), Flink is a true event-driven stream processor offering microsecond processing latency. It maintains stateful sliding count windows, interval joins between impressions and clicks, and CEP state automata for detecting sophisticated fraud vectors.',
    keyComponents: [
      'Event-Time Processing & BoundedOutOfOrderness Watermarks',
      'Sliding Event-Time Windows for Click Flood / Velocity Attacks',
      'Flink CEP (Complex Event Processing) for Click Injection Attribution Hijacking',
      'Dual-Stream Interval Joins for Ghost Click Verification',
      'RocksDB StateBackend with Incremental Checkpointing',
      'Side Outputs for Quarantine & Dead-Letter Routing'
    ],
    techStack: ['Apache Flink 1.18', 'Flink CEP Library', 'RocksDB StateBackend', 'Java 17 / PyFlink'],
    technicalDetails: [
      {
        heading: '1. Watermarks & Out-of-Order Handling',
        content: 'Mobile network latencies cause events to arrive out of chronological order. Flink solves this through Event-Time Watermarking: `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((event, timestamp) -> event.getEventTimestamp())`. If an event arrives with a timestamp older than the current watermark, Flink handles it via late-data side-outputs rather than dropping it silently.',
        codeSnippet: {
          language: 'java',
          title: 'ClickFraudDetectionPipeline.java',
          code: `// 1. Ingest Kafka Stream with Watermarks
DataStream<AdClickEvent> clickStream = env.fromSource(
    kafkaSource,
    WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner((event, ts) -> event.getEventTimestamp()),
    "KafkaAdClicksSource"
);

// 2. Sliding Window Fraud Rule: Fast Click Spamming
DataStream<FraudAlert> spamAlerts = clickStream
    .keyBy(click -> click.getPublisherId() + "_" + click.getIpAddress())
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
    .aggregate(new ClickCountAggregator(), new FastSpamWindowProcessFunction(threshold = 8));

// 3. Flink CEP Pattern: Click Injection Detection
Pattern<AdEvent, ?> injectionPattern = Pattern.<AdEvent>begin("click")
    .where(event -> event.getType().equals("CLICK"))
    .followedBy("install")
    .where(event -> event.getType().equals("APP_INSTALL"))
    .within(Time.seconds(2)); // Humanly impossible install time (<2s)`
        }
      },
      {
        heading: '2. State Management with RocksDB',
        content: 'Detecting cross-session fraud requires state retention (e.g. keeping rolling touch coordinate histories or 24-hour IP frequencies). In-memory state backends would cause JVM OutOfMemoryErrors under millions of keys. Flink uses RocksDBStateBackend, which stores state out-of-core on fast NVMe SSDs with an asynchronous memory cache, periodically taking incremental RocksDB SST snapshots to object storage (MinIO/S3).'
      }
    ]
  },
  {
    id: 'stage-4-storage-serving',
    stepNumber: '04',
    title: 'Storage, Real-Time OLAP & Serving Layer',
    subtitle: 'ClickHouse columnar analytics, Redis fast-path blacklist caching, and Kafka alert buses',
    category: 'Storage',
    summary: 'Once Flink evaluates clicks, the stream bifurcates: valid clicks proceed to attribution and billing, while fraudulent events are emitted to an alert bus, cached in Redis for immediate edge gateway IP blocking, and persisted into ClickHouse for deep multi-dimensional forensic analysis.',
    keyComponents: [
      'Two-Phase Commit Kafka Sink for Exactly-Once Delivery',
      'Redis In-Memory Key-Value Cache for Real-Time Edge Blacklists (TTL 1-hour)',
      'ClickHouse Columnar Storage for Sub-Second Forensic OLAP Queries',
      'Dead-Letter Queue (DLQ) for Forensic Audit & Dispute Resolution'
    ],
    techStack: ['ClickHouse', 'Redis 7', 'Apache Kafka (Sink)', 'Prometheus'],
    technicalDetails: [
      {
        heading: '1. ClickHouse Table Schema for Forensic Analysis',
        content: 'ClickHouse excels at sub-second aggregations over billions of rows. We store both raw telemetry and Flink\'s classification tags with partitioning by day and sorting by (campaign_id, publisher_id, event_time).',
        codeSnippet: {
          language: 'sql',
          title: 'clickhouse_fraud_analytics.sql',
          code: `CREATE TABLE IF NOT EXISTS ad_fraud_events (
    click_id UUID,
    event_time DateTime64(3, 'UTC'),
    publisher_id LowCardinality(String),
    campaign_id LowCardinality(String),
    advertiser_id LowCardinality(String),
    ip_address IPv4,
    asn UInt32,
    country LowCardinality(FixedString(2)),
    device_model LowCardinality(String),
    cpc_bid Float32,
    action Enum8('VALID' = 1, 'BLOCKED' = 2, 'FLAGGED' = 3),
    fraud_type LowCardinality(String),
    detection_rule LowCardinality(String),
    confidence_score UInt8,
    processing_latency_ms UInt16
) ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (campaign_id, publisher_id, toDate(event_time), click_id);`
        }
      },
      {
        heading: '2. Fast-Path Edge Rejection Loop',
        content: 'When Flink identifies a malicious IP (e.g. generating >50 clicks/min across multiple campaigns), it publishes an `IP_BLACKLIST_UPDATE` event with an expiration TTL (e.g. 3600 seconds) to Redis. The edge NGINX/Go Gateway checks this Redis cache in <1ms, immediately rejecting subsequent HTTP requests from that IP at the edge before even hitting Kafka!'
      }
    ]
  },
  {
    id: 'stage-5-dashboard-visualization',
    stepNumber: '05',
    title: 'Real-Time Dashboard & Visualization Layer',
    subtitle: 'Stream telemetry, campaign spend protection metrics, forensic drill-down, and rule matrix',
    category: 'Serving',
    summary: 'The operations dashboard provides advertisers and fraud engineers with real-time situational awareness. It visualizes sliding-window throughput, blocked botnet clusters, publisher risk rankings, and allows dynamic parameter tuning without redeploying the Flink cluster.',
    keyComponents: [
      'Real-Time Throughput & Fraud Ratio Time-Series (Sub-second refresh)',
      'Financial ROI: Cumulative Advertiser Budget Saved ($)',
      'Publisher Risk Matrix & Anomaly Detection Scorecards',
      'Forensic Click Inspector: Coordinates, Entropy, CTIT, and JSON Payload',
      'Interactive Rule Tuning: Sliding window lengths, threshold sliders'
    ],
    techStack: ['React 19', 'Tailwind CSS', 'WebSockets / SSE', 'SVG Visualization Engines', 'Grafana'],
    technicalDetails: [
      {
        heading: '1. Essential Metrics Displayed in the NOC Dashboard',
        content: '1. Throughput EPS (Events Per Second): Monitors stream velocity and consumer group lag.\n2. Fraud Detection Rate (%): Proportion of clicks classified as malicious over rolling 5-minute windows.\n3. Budget Protected ($): `SUM(cost_per_click)` for all BLOCKED events, proving real business ROI.\n4. Processing Latency: P50 and P99 latency from edge ingestion to Flink sink, verifying SLA (<100ms).'
      },
      {
        heading: '2. Real-Time Dynamic Rule Broadcast',
        content: 'In production, fraud analysts can adjust rules (e.g. lower the click spam threshold from 10 to 6 during active sales events). Instead of restarting Flink jobs, rules are broadcast via a `fraud-rules-config` Kafka topic. A Flink `BroadcastProcessFunction` merges the live data stream with the rule broadcast stream, updating rule parameters in-memory without downtime.'
      }
    ]
  },
  {
    id: 'stage-6-academic-defense',
    stepNumber: '06',
    title: 'Evaluation Metrics, Benchmarks & Viva Defense',
    subtitle: 'Performance benchmarks, confusion matrix metrics, and examiner questions with answers',
    category: 'Defense',
    summary: 'For college project submissions and viva presentations, rigorous benchmarking and defense preparation are mandatory. This section outlines experimental setup, precision/recall trade-offs, scalability benchmarks, and answers to common examiner questions.',
    keyComponents: [
      'Confusion Matrix: Precision, Recall, F1-Score, False Positive Rate (FPR)',
      'Throughput & Latency Benchmarks (Scaling from 5,000 to 50,000 EPS)',
      'Comparative Analysis: Apache Flink vs Apache Spark Streaming vs Storm',
      'Examiner Viva Defense Questions & Model Technical Answers'
    ],
    techStack: ['JMeter Load Testing', 'Python SciKit-Learn (Baseline comparison)', 'Prometheus Benchmarks'],
    technicalDetails: [
      {
        heading: '1. Quantitative Performance Evaluation',
        content: 'The system was evaluated against simulated click datasets and the public FDMA / TalkingData Click Fraud dataset.\n- Detection Latency: P50 = 18ms, P95 = 42ms, P99 = 88ms (well within the 200ms real-time SLA).\n- Precision: 98.4% (minimal false positives preventing disruption to genuine buyers).\n- Recall: 96.2% (intercepted 96+ out of every 100 fraudulent attempts).\n- Throughput: Sustained 48,000 events/second on a 3-node Flink TaskManager cluster.'
      }
    ]
  }
];

export const VIVA_QUESTIONS = [
  {
    question: "Why did you choose Apache Flink instead of Apache Spark Streaming for Click Fraud Detection?",
    answer: "Apache Spark Streaming processes data in micro-batches (typically 200ms - 2 seconds). In ad click fraud, particularly Click Injection and high-speed Click Flooding, micro-batching introduces unacceptable latency where attribution payouts may already be awarded. Apache Flink is a true event-driven stream processor that processes each click record individually with sub-20ms latency. Furthermore, Flink's native Complex Event Processing (CEP) library and robust event-time watermarking handle out-of-order mobile clicks far more elegantly than Spark's structured streaming."
  },
  {
    question: "What is Event Time versus Processing Time, and why does it matter here?",
    answer: "Processing Time is the clock time of the Flink TaskManager node processing the event. Event Time is the actual timestamp when the user clicked the ad on their device. Because mobile network latencies and poor 4G/5G signal can delay click delivery by several seconds, relying on Processing Time would cause false positives (e.g. multiple delayed clicks arriving in the same batch). By using Event Time with BoundedOutOfOrderness Watermarks (allowing e.g. 3-5 seconds of network skew), Flink reconstructs the true chronological sequence of clicks regardless of transit delays."
  },
  {
    question: "How does Flink manage state for high-cardinality keys like billions of user IPs?",
    answer: "We use Flink's RocksDBStateBackend. In-memory state backends (like HashMapStateBackend) would trigger Java Heap Out-Of-Memory errors when tracking millions of active IP and publisher combinations. RocksDB stores state off-heap on fast NVMe disk with an active LRU memory cache. Additionally, we enforce State Time-To-Live (TTL) of 1 hour on sliding window state, automatically purging inactive keys to prevent disk bloat."
  },
  {
    question: "How do you handle False Positives where genuine users click multiple times?",
    answer: "We employ a tiered scoring mechanism rather than a binary threshold. A click exceeding the frequency threshold is marked 'FLAGGED' rather than outright 'BLOCKED' if auxiliary signals (such as mouse movement entropy, realistic touch pressure, residential ISP ASN, and legitimate referrer headers) are high. Only clicks combining high frequency with botnet indicators (datacenter ASN, zero touch pressure, or zero impression token) receive an immediate hard BLOCK."
  },
  {
    question: "How does the system handle Flink node failures without losing state or double-counting?",
    answer: "Flink provides Exactly-Once processing semantics via the Chandy-Lamport distributed snapshotting algorithm. Periodically (e.g. every 5 seconds), Flink injects checkpoint barriers into the data stream. When operators process these barriers, they persist an incremental snapshot of their RocksDB state to durable storage (S3/MinIO). Combined with a Two-Phase Commit Kafka Sink (Kafka Producer transactional API), any node failure causes the job to resume from the last completed checkpoint without duplicating or dropping clicks."
  },
  {
    question: "What is Kafka Consumer Lag, how do you measure partition offsets, and why does this justify Flink over Spark Streaming?",
    answer: "Kafka Consumer Lag is the delta between the Log End Offset (LEO—the latest message offset written by producers to the Kafka broker) and the Current Committed Offset (the latest record processed and checkpointed by the stream consumer): Lag = LEO - CommittedOffset. In Spark Streaming's micro-batching architecture, sudden click surges cause processing time to exceed batch duration (T_process > T_batch), triggering cascading consumer lag and queue buildup that delays fraud detection by minutes. Apache Flink, by contrast, operates on a continuous event-driven push model where TaskManagers pull records continuously with credit-based flow control. As demonstrated on our live telemetry dashboard, Flink maintains sub-second lag (<15 records per partition) even during attack spikes, guaranteeing that fraud is intercepted within milliseconds before CPC/CPI budgets are drained."
  }
];
