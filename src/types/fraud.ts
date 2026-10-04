export type FraudType = 
  | 'CLICK_SPAMMING'
  | 'CLICK_INJECTION'
  | 'DATACENTER_BOT'
  | 'GHOST_CLICK'
  | 'COORDINATED_FARM'
  | 'NONE';

export type RiskLevel = 'LOW' | 'MEDIUM' | 'HIGH';
export type ActionStatus = 'VALID' | 'FLAGGED' | 'BLOCKED';

export interface BehavioralFeatures {
  // Feature 1: Click Frequency
  clicksPerMinuteUser: number;
  // Feature 2: IP Click Frequency
  clicksFromSameIp: number;
  // Feature 3: Device Click Frequency
  clicksPerDevice: number;
  // Feature 4: Unique Users per IP
  uniqueUsersPerIp: number;
  // Feature 5: Unique Devices per IP
  uniqueDevicesPerIp: number;
  // Feature 6: Time Between Clicks
  timeSincePreviousClickMs: number;
  // Feature 7: Repeated Advertisement
  sameAdClickCount: number;
  // Feature 8: Click Diversity
  uniqueAdsClicked: number;
  // Feature 9: Geographic Anomaly
  isGeoAnomaly: boolean;
  // Feature 10: Bot-Like Behavior Score (0-100)
  botBehaviorScore: number;
}

export interface AdClickEvent {
  // Core Identifiers (answers: Who, Which Ad, When, Where)
  eventId: string;
  clickId: string;
  timestamp: string; // ISO 8601
  epochMs: number;
  userId: string;
  adId: string;
  campaignId: string;
  campaignName: string;
  publisherId: string;
  publisherName: string;
  costPerClick: number; // in USD

  // TalkingData Dataset Specific Fields
  appId: string; // e.g. "APP_3"
  channelId: string; // e.g. "CH_107"
  deviceType: string; // e.g. "DEV_1" (mobile, tablet, desktop)
  osVersion: string; // e.g. "OS_19" (Android 14)
  browser: string; // e.g. "Chrome 124"

  // Network & Biometrics
  ipAddress: string;
  asn: string;
  country: string;
  city: string;
  clickPosition: string; // banner, interstitial, native, footer
  referrer: string; // google.com, facebook, direct
  clickCoordinates: { x: number; y: number };
  touchPressure?: number;
  timeSinceImpressionMs: number;
  clickToInstallTimeMs?: number;

  // The 10 Stream Feature Engineering Metrics
  features: BehavioralFeatures;

  // Hybrid Decision Engine (ML + Rules)
  ruleScore: number; // 0 to 100
  mlProbability: number; // 0.00 to 1.00 (from Random Forest / XGBoost)
  riskScore: number; // Combined 0 to 100
  riskLevel: RiskLevel; // LOW (0-30), MEDIUM (31-60), HIGH (61-100)
  action: ActionStatus; // VALID, FLAGGED, BLOCKED
  detectedFraudType: FraudType;
  reasons: string[]; // Explanatory breakdown
  processingLatencyMs: number; // Stream execution latency
}

export interface FlinkEngineState {
  slidingWindowSeconds: number;
  clickThresholdPerWindow: number;
  minAllowedCtitMs: number;
  maxVelocityAllowed: number;
  ghostClickToleranceMs: number;
  entropyCutoff: number;
  ruleWeight: number; // e.g. 0.4
  mlWeight: number; // e.g. 0.6
  totalIngested: number;
  totalFraud: number;
  totalValid: number;
  totalBudgetProtected: number;
  currentThroughputEps: number;
  watermarkDelayMs: number;
  activeRocksDbKeys: number;
}

export interface FraudPatternDefinition {
  id: FraudType;
  name: string;
  severity: 'CRITICAL' | 'HIGH' | 'MEDIUM';
  description: string;
  industryImpact: string;
  flinkMechanism: string;
  windowType: string;
  sampleIndicator: string;
}

export interface KafkaPartitionMetric {
  partitionId: number;
  topic: string;
  logEndOffset: number; // LEO (newest offset produced to broker)
  committedOffset: number; // Current offset consumed & checkpointed by Flink
  lag: number; // logEndOffset - committedOffset
  assignedSlot: string; // e.g. "TaskManager-01 [Slot 0]"
  brokerNode: string; // e.g. "broker-1:9092"
  eps: number;
  status: 'OPTIMAL' | 'ELEVATED' | 'HIGH_LAG';
  progressPct: number;
}

export interface KafkaConsumerLagState {
  consumerGroup: string;
  topic: string;
  totalLogEndOffset: number;
  totalCommittedOffset: number;
  totalLag: number;
  avgFetchLatencyMs: number;
  producerRateEps: number;
  consumerRateEps: number;
  backpressureRatio: number; // 0.0 to 1.0 (Flink credit-based flow)
  checkpointId: number;
  lastCheckpointDurationMs: number;
  partitions: KafkaPartitionMetric[];
  lagHistory: {
    time: string;
    totalLag: number;
    producerEps: number;
    consumerEps: number;
  }[];
}

