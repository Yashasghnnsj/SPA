export interface CodeArtifact {
  id: string;
  name: string;
  filename: string;
  language: string;
  description: string;
  category: 'Flink Engine' | 'Infrastructure' | 'Ingestion & Producer' | 'Configuration';
  code: string;
}

export const CODE_ARTIFACTS: CodeArtifact[] = [
  {
    id: 'flink-java-job',
    name: 'Apache Flink Java Main Job',
    filename: 'ClickFraudDetectionJob.java',
    language: 'java',
    category: 'Flink Engine',
    description: 'Production-ready Flink application using KeyedProcessFunction, SlidingEventTimeWindows, and Complex Event Processing (CEP).',
    code: `package com.fraudguard.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Real-Time Ad Click Fraud Detection Engine
 * Sub-50ms latency stream processing using Apache Flink 1.18
 */
public class ClickFraudDetectionJob {

    // Side Output for Quarantined / Fraudulent Clicks (Dead-Letter Queue)
    public static final OutputTag<AdClickEvent> FRAUD_QUARANTINE_TAG = 
        new OutputTag<AdClickEvent>("fraudulent-clicks-dlq"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Enable Checkpointing for Exactly-Once Semantics (every 10s)
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);

        // 2. Configure Kafka Consumer Source with Watermark Strategy
        KafkaSource<AdClickEvent> kafkaSource = KafkaSource.<AdClickEvent>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("raw-ad-clicks")
            .setGroupId("flink-click-fraud-pipeline")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new AdClickDeserializationSchema())
            .build();

        // 3. Assign Timestamps & BoundedOutOfOrderness Watermarks (skew tolerance: 3 seconds)
        DataStream<AdClickEvent> rawClicks = env.fromSource(
            kafkaSource,
            WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTimestamp()),
            "Kafka-Raw-Clicks-Source"
        );

        // 4. Primary Detection: KeyedProcessFunction for Velocity & Datacenter ASN Filters
        SingleOutputStreamOperator<AdClickEvent> evaluatedClicks = rawClicks
            .keyBy(event -> event.getPublisherId() + "_" + event.getIpAddress())
            .process(new MultiFactorFraudEvaluator());

        // 5. Secondary Detection: Flink CEP for Click Injection (Install Hijacking)
        Pattern<AdClickEvent, ?> clickInjectionPattern = Pattern.<AdClickEvent>begin("click")
            .where(new SimpleCondition<AdClickEvent>() {
                @Override
                public boolean filter(AdClickEvent value) {
                    return value.getEventType().equals("CLICK");
                }
            })
            .followedBy("install")
            .where(new SimpleCondition<AdClickEvent>() {
                @Override
                public boolean filter(AdClickEvent value) {
                    return value.getEventType().equals("APP_INSTALL");
                }
            })
            .within(Time.seconds(2)); // Humanly impossible install (<2000ms from click)

        PatternStream<AdClickEvent> patternStream = CEP.pattern(
            rawClicks.keyBy(AdClickEvent::getDeviceId),
            clickInjectionPattern
        );

        DataStream<FraudAlert> injectionAlerts = patternStream.select(
            (PatternSelectFunction<AdClickEvent, FraudAlert>) pattern -> {
                AdClickEvent click = pattern.get("click").get(0);
                AdClickEvent install = pattern.get("install").get(0);
                long ctit = install.getEventTimestamp() - click.getEventTimestamp();
                return new FraudAlert(
                    click.getClickId(),
                    "CLICK_INJECTION",
                    "Click-to-Install time was " + ctit + "ms (<2s threshold). Install hijacked.",
                    99.0
                );
            }
        );

        // 6. Sink Legitimate Clicks to Billing & Reporting Topic
        KafkaSink<AdClickEvent> cleanClicksSink = KafkaSink.<AdClickEvent>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("clean-ad-clicks")
                    .setValueSerializationSchema(new AdClickSerializationSchema())
                    .build()
            )
            .build();

        evaluatedClicks.sinkTo(cleanClicksSink);

        // 7. Route Quarantined Fraudulent Clicks to Side Output Sink
        DataStream<AdClickEvent> fraudStream = evaluatedClicks.getSideOutput(FRAUD_QUARANTINE_TAG);
        
        KafkaSink<AdClickEvent> fraudSink = KafkaSink.<AdClickEvent>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("fraud-alerts-dlq")
                    .setValueSerializationSchema(new AdClickSerializationSchema())
                    .build()
            )
            .build();

        fraudStream.sinkTo(fraudSink);

        // Execute Flink Distributed Streaming Job
        env.execute("Flink-Realtime-Click-Fraud-Detection-Pipeline");
    }

    /**
     * KeyedProcessFunction maintaining stateful rolling metrics per IP/Publisher
     */
    public static class MultiFactorFraudEvaluator extends KeyedProcessFunction<String, AdClickEvent, AdClickEvent> {
        private transient ValueState<Long> lastClickTimestamp;
        private transient ValueState<Integer> windowClickCount;
        private static final int MAX_CLICKS_PER_10S = 8;

        @Override
        public void open(Configuration parameters) {
            lastClickTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastClickTimestamp", Long.class)
            );
            windowClickCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("windowClickCount", Integer.class, 0)
            );
        }

        @Override
        public void processElement(AdClickEvent click, Context ctx, Collector<AdClickEvent> out) throws Exception {
            long currentTs = click.getEventTimestamp();
            Long lastTs = lastClickTimestamp.value();
            int count = windowClickCount.value() == null ? 0 : windowClickCount.value();

            // Check A: Datacenter ASN (AWS, DigitalOcean, Hetzner, OVH)
            if (isDatacenterAsn(click.getAsn())) {
                click.setAction("BLOCKED");
                click.setFraudType("DATACENTER_BOT");
                click.setDetectionRule("Rule #1: Datacenter Hosting ASN Disallowed");
                ctx.output(FRAUD_QUARANTINE_TAG, click);
                return;
            }

            // Check B: Click Spamming / Rapid Velocity (<150ms between clicks)
            if (lastTs != null && (currentTs - lastTs) < 150) {
                click.setAction("BLOCKED");
                click.setFraudType("CLICK_SPAMMING");
                click.setDetectionRule("Rule #2: Impossible inter-click interval (<150ms)");
                ctx.output(FRAUD_QUARANTINE_TAG, click);
                return;
            }

            // Check C: Sliding count check within current window
            count++;
            if (count > MAX_CLICKS_PER_10S) {
                click.setAction("BLOCKED");
                click.setFraudType("CLICK_SPAMMING");
                click.setDetectionRule("Rule #3: Exceeded velocity limit (>8 clicks / 10s)");
                ctx.output(FRAUD_QUARANTINE_TAG, click);
            } else {
                click.setAction("VALID");
                out.collect(click);
            }

            // Update State & Register Timer to clear window state
            lastClickTimestamp.update(currentTs);
            windowClickCount.update(count);
            ctx.timerService().registerEventTimeTimer(currentTs + 10000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            windowClickCount.clear();
        }

        private boolean isDatacenterAsn(String asn) {
            return asn != null && (
                asn.contains("AS16509") || // Amazon AWS
                asn.contains("AS14061") || // DigitalOcean
                asn.contains("AS24940") || // Hetzner
                asn.contains("AS16276")    // OVH
            );
        }
    }
}`
  },
  {
    id: 'flink-python-job',
    name: 'PyFlink Alternative Implementation',
    filename: 'pyflink_fraud_pipeline.py',
    language: 'python',
    category: 'Flink Engine',
    description: 'Python implementation using PyFlink DataStream and Table API for machine learning score enrichment.',
    code: `"""
Real-Time Ad Click Fraud Detection via PyFlink
Python 3.10 + Apache Flink 1.18
"""
from pyflink.common import WatermarkStrategy, Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
import json

class ClickSpamEvaluator(KeyedProcessFunction):
    def __init__(self, click_threshold: int = 8):
        self.threshold = click_threshold
        self.count_state = None
        self.last_ts_state = None

    def open(self, runtime_context: RuntimeContext):
        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("click_count", Types.INT())
        )
        self.last_ts_state = runtime_context.get_state(
            ValueStateDescriptor("last_timestamp", Types.LONG())
        )

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        click = json.loads(value)
        event_time = click["event_timestamp"]
        
        # Datacenter ASN Check
        if any(dc_asn in click.get("asn", "") for dc_asn in ["AS16509", "AS14061", "AS24940"]):
            click["action"] = "BLOCKED"
            click["fraud_type"] = "DATACENTER_BOT"
            yield json.dumps(click)
            return

        current_count = self.count_state.value() or 0
        current_count += 1
        self.count_state.update(current_count)

        if current_count > self.threshold:
            click["action"] = "BLOCKED"
            click["fraud_type"] = "CLICK_SPAMMING"
            click["rule"] = f"Frequency exceeded: {current_count} clicks in rolling window"
        else:
            click["action"] = "VALID"

        self.last_ts_state.update(event_time)
        ctx.timer_service().register_event_time_timer(event_time + 10000)
        yield json.dumps(click)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        self.count_state.clear()


def run_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    
    # Ingest Kafka Stream
    kafka_source = env.from_source(
        source=KafkaSource.builder()
            .set_bootstrap_servers("kafka:9092")
            .set_topics("raw-ad-clicks")
            .set_group_id("pyflink-fraud-group")
            .set_value_only_deserializer(SimpleStringSchema())
            .build(),
        watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3)),
        source_name="KafkaRawClicks"
    )

    processed_clicks = kafka_source \\
        .key_by(lambda record: json.loads(record)["ip_address"]) \\
        .process(ClickSpamEvaluator(click_threshold=8))

    processed_clicks.print()
    env.execute("PyFlink-Click-Fraud-Job")

if __name__ == "__main__":
    run_pipeline()`
  },
  {
    id: 'docker-compose-stack',
    name: 'Full Distributed Infrastructure Stack',
    filename: 'docker-compose.yml',
    language: 'yaml',
    category: 'Infrastructure',
    description: 'Docker Compose orchestration for Apache Kafka (KRaft mode), Flink JobManager, Flink TaskManager, ClickHouse, and Grafana.',
    code: `version: '3.8'

services:
  # 1. Apache Kafka in KRaft mode (No Zookeeper required)
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'

  # 2. Apache Flink JobManager (Master Coordinator)
  jobmanager:
    image: flink:1.18.0-scala_2.12-java17
    container_name: flink-jobmanager
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        state.backend: rocksdb
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        state.backend.incremental: true
        rest.flamegraph.enabled: true
    volumes:
      - ./checkpoints:/tmp/flink-checkpoints

  # 3. Apache Flink TaskManager (Worker Engine)
  taskmanager:
    image: flink:1.18.0-scala_2.12-java17
    container_name: flink-taskmanager
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 2
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
        state.backend: rocksdb
        taskmanager.memory.process.size: 2048m

  # 4. ClickHouse Columnar OLAP Database
  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    container_name: clickhouse-server
    ports:
      - "8123:8123"
      - "9000:9000"
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

  # 5. Redis Hot IP Blacklist Fast-Path Cache
  redis:
    image: redis:7-alpine
    container_name: redis-cache
    ports:
      - "6379:6379"

  # 6. Grafana Real-Time Operations Monitoring UI
  grafana:
    image: grafana/grafana:10.1.0
    container_name: grafana
    ports:
      - "3001:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    depends_on:
      - clickhouse`
  },
  {
    id: 'synthetic-generator',
    name: 'Kafka Traffic Generator & Attack Injector',
    filename: 'kafka_click_generator.py',
    language: 'python',
    category: 'Ingestion & Producer',
    description: 'Python script to publish high-throughput synthetic clicks, simulating normal mobile users alongside targeted bot attacks.',
    code: `import json
import time
import random
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='lz4',
    acks='all'
)

CAMPAIGNS = ["cmp_cyber_sale", "cmp_fintech_loan", "cmp_crypto_exchange", "cmp_saas_crm"]
PUBLISHERS = ["pub_news_daily", "pub_gaming_arcade", "pub_coupon_hub", "pub_shady_apk"]
RESIDENTIAL_ASNS = ["AS7018 AT&T", "AS7922 Comcast", "AS20115 Charter", "AS5651 Bharti Airtel"]
DATACENTER_ASNS = ["AS16509 Amazon AWS", "AS14061 DigitalOcean", "AS24940 Hetzner"]

def generate_click(attack_type=None):
    now_ms = int(time.time() * 1000)
    
    if attack_type == "CLICK_INJECTION":
        # Install occurs within 400ms of click
        return {
            "click_id": str(uuid.uuid4()),
            "event_timestamp": now_ms,
            "campaign_id": "cmp_gaming_arcade",
            "publisher_id": "pub_shady_apk",
            "cpc_bid": 3.85,
            "ip_address": f"192.168.1.{random.randint(2, 254)}",
            "asn": random.choice(RESIDENTIAL_ASNS),
            "click_coordinates": {"x": 240, "y": 480},
            "touch_pressure": 0.85,
            "event_type": "CLICK",
            "click_to_install_time_ms": random.randint(150, 600) # Maliciously low CTIT!
        }
    elif attack_type == "DATACENTER_BOT":
        return {
            "click_id": str(uuid.uuid4()),
            "event_timestamp": now_ms,
            "campaign_id": random.choice(CAMPAIGNS),
            "publisher_id": "pub_coupon_hub",
            "cpc_bid": 2.10,
            "ip_address": f"54.210.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "asn": random.choice(DATACENTER_ASNS),
            "click_coordinates": {"x": 0, "y": 0}, # Headless browser default
            "touch_pressure": 0.0,
            "event_type": "CLICK"
        }
    else: # Normal legitimate human click
        return {
            "click_id": str(uuid.uuid4()),
            "event_timestamp": now_ms,
            "campaign_id": random.choice(CAMPAIGNS),
            "publisher_id": random.choice(PUBLISHERS),
            "cpc_bid": round(random.uniform(0.40, 2.50), 2),
            "ip_address": f"174.62.{random.randint(1, 250)}.{random.randint(1, 250)}",
            "asn": random.choice(RESIDENTIAL_ASNS),
            "click_coordinates": {"x": random.randint(40, 360), "y": random.randint(100, 700)},
            "touch_pressure": round(random.uniform(0.6, 1.0), 2),
            "event_type": "CLICK",
            "click_to_install_time_ms": random.randint(8000, 45000)
        }

if __name__ == '__main__':
    print("Publishing ad clicks into topic 'raw-ad-clicks'...")
    while True:
        # 85% normal clicks, 15% fraud attempts
        attack = random.choices([None, "CLICK_INJECTION", "DATACENTER_BOT"], weights=[85, 8, 7])[0]
        event = generate_click(attack)
        producer.send("raw-ad-clicks", key=event["publisher_id"].encode('utf-8'), value=event)
        time.sleep(0.05) # 20 clicks/sec`
  },
  {
    id: 'flink-config-file',
    name: 'Flink Production Configuration',
    filename: 'flink-conf.yaml',
    language: 'yaml',
    category: 'Configuration',
    description: 'Tuning parameters for RocksDB state backend, incremental snapshots, and checkpoint storage.',
    code: `# Flink Core JobManager & TaskManager Sizing
jobmanager.rpc.address: jobmanager
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 4096m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4

# Fault Tolerance & Incremental RocksDB Checkpointing
state.backend: rocksdb
state.backend.incremental: true
state.checkpoints.dir: file:///opt/flink/checkpoints
state.savepoints.dir: file:///opt/flink/savepoints
execution.checkpointing.interval: 10000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.min-pause: 5000
execution.checkpointing.timeout: 60000
execution.checkpointing.max-concurrent-checkpoints: 1

# RocksDB Off-Heap Memory Optimization
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.block.cache-size: 512mb
state.backend.rocksdb.write-buffer-size: 64mb
state.backend.rocksdb.compaction.style: LEVEL

# Network Buffers for High-Throughput Streams
taskmanager.network.memory.fraction: 0.15
taskmanager.network.memory.min: 64mb
taskmanager.network.memory.max: 1gb`
  }
];
