import { AdClickEvent, BehavioralFeatures, FlinkEngineState, FraudType, RiskLevel, ActionStatus, KafkaConsumerLagState, KafkaPartitionMetric } from '../types/fraud';

const CAMPAIGNS = [
  { id: 'CAMP20', name: 'Nike Festival Mega Sale', adId: 'AD501', advertiser: 'Nike Direct', cpc: 2.45, appId: 'APP_3', channelId: 'CH_107' },
  { id: 'CAMP21', name: 'UberEats 50% Off First 3', adId: 'AD502', advertiser: 'Uber Technologies', cpc: 3.80, appId: 'APP_12', channelId: 'CH_210' },
  { id: 'CAMP22', name: 'Revolut Metal Zero Fee', adId: 'AD503', advertiser: 'Revolut Bank', cpc: 5.20, appId: 'APP_18', channelId: 'CH_340' },
  { id: 'CAMP23', name: 'Shopify 90-Day Free Trial', adId: 'AD504', advertiser: 'Shopify Inc', cpc: 4.10, appId: 'APP_7', channelId: 'CH_115' },
  { id: 'CAMP24', name: 'Fortnite Mobile Season 9', adId: 'AD505', advertiser: 'Epic Games', cpc: 1.90, appId: 'APP_24', channelId: 'CH_189' },
];

const PUBLISHERS = [
  { id: 'PUB101', name: 'The New York Times App', trust: 0.98 },
  { id: 'PUB102', name: 'Reddit Mobile Feed', trust: 0.95 },
  { id: 'PUB103', name: 'Helix Jump & Arcade Games', trust: 0.85 },
  { id: 'PUB104', name: 'Modded APK Downloader Net', trust: 0.20 },
  { id: 'PUB105', name: 'Free Reward Points Club', trust: 0.30 },
];

const RESIDENTIAL_NETWORKS = [
  { asn: 'AS7018 AT&T Services', country: 'United States', city: 'Atlanta', ipPrefix: '174.62' },
  { asn: 'AS7922 Comcast Cable', country: 'United States', city: 'Philadelphia', ipPrefix: '73.18' },
  { asn: 'AS5651 Bharti Airtel', country: 'India', city: 'Mumbai', ipPrefix: '103.25' },
  { asn: 'AS2856 British Telecom', country: 'United Kingdom', city: 'London', ipPrefix: '81.149' },
  { asn: 'AS3320 Deutsche Telekom', country: 'Germany', city: 'Frankfurt', ipPrefix: '217.80' },
];

const DATACENTER_NETWORKS = [
  { asn: 'AS16509 Amazon AWS Datacenter', country: 'United States', city: 'Ashburn', ipPrefix: '54.210' },
  { asn: 'AS14061 DigitalOcean NYC Datacenter', country: 'United States', city: 'New York', ipPrefix: '167.99' },
  { asn: 'AS24940 Hetzner Online Datacenter', country: 'Germany', city: 'Falkenstein', ipPrefix: '88.198' },
];

const DEVICES = [
  { id: 'DEV889', type: 'mobile', browser: 'Chrome 124', os: 'Android 14' },
  { id: 'DEV412', type: 'mobile', browser: 'Mobile Safari', os: 'iOS 17.4' },
  { id: 'DEV1023', type: 'desktop', browser: 'Edge 122', os: 'Windows 11' },
  { id: 'DEV991', type: 'tablet', browser: 'Safari 17', os: 'iPadOS 17' },
  { id: 'DEV_BOT_01', type: 'headless', browser: 'Headless Chrome', os: 'Linux 5.15' },
];

// Rolling state tracking per IP and User for Apache Flink window emulation
interface RollingState {
  timestamps: number[];
  users: Set<string>;
  devices: Set<string>;
  ads: Set<string>;
  lastAd: string;
  adCounts: Record<string, number>;
  lastCountry: string;
}

const ipStateStore = new Map<string, RollingState>();
const userStateStore = new Map<string, RollingState>();

let eventSequence = 100234;

export class FlinkStreamEngine {
  private state: FlinkEngineState;
  private kafkaState: KafkaConsumerLagState;
  private listeners: ((event: AdClickEvent, engineState: FlinkEngineState, kafkaState: KafkaConsumerLagState) => void)[] = [];
  private isRunning: boolean = true;
  private timer: number | null = null;
  private speedMs: number = 850;
  private activeAttackQueue: FraudType[] = [];
  private checkpointCounter: number = 248;

  constructor(initialSettings?: Partial<FlinkEngineState>) {
    this.state = {
      slidingWindowSeconds: 10,
      clickThresholdPerWindow: 8,
      minAllowedCtitMs: 1800,
      maxVelocityAllowed: 1000, // 1 second
      ghostClickToleranceMs: 60000,
      entropyCutoff: 1.4,
      ruleWeight: 0.4,
      mlWeight: 0.6,
      totalIngested: 0,
      totalFraud: 0,
      totalValid: 0,
      totalBudgetProtected: 0,
      currentThroughputEps: 28,
      watermarkDelayMs: 32,
      activeRocksDbKeys: 1840,
      ...initialSettings
    };

    // Initialize 6 Kafka Partitions for topic 'ad-click-events'
    const initialPartitions: KafkaPartitionMetric[] = [
      { partitionId: 0, topic: 'ad-click-events', logEndOffset: 184520, committedOffset: 184518, lag: 2, assignedSlot: 'TaskManager-01 [Slot 0]', brokerNode: 'broker-1:9092', eps: 5.2, status: 'OPTIMAL', progressPct: 99.9 },
      { partitionId: 1, topic: 'ad-click-events', logEndOffset: 184490, committedOffset: 184487, lag: 3, assignedSlot: 'TaskManager-01 [Slot 1]', brokerNode: 'broker-1:9092', eps: 4.8, status: 'OPTIMAL', progressPct: 99.9 },
      { partitionId: 2, topic: 'ad-click-events', logEndOffset: 184610, committedOffset: 184606, lag: 4, assignedSlot: 'TaskManager-02 [Slot 0]', brokerNode: 'broker-2:9092', eps: 5.4, status: 'OPTIMAL', progressPct: 99.8 },
      { partitionId: 3, topic: 'ad-click-events', logEndOffset: 184430, committedOffset: 184428, lag: 2, assignedSlot: 'TaskManager-02 [Slot 1]', brokerNode: 'broker-2:9092', eps: 4.6, status: 'OPTIMAL', progressPct: 99.9 },
      { partitionId: 4, topic: 'ad-click-events', logEndOffset: 184580, committedOffset: 184575, lag: 5, assignedSlot: 'TaskManager-03 [Slot 0]', brokerNode: 'broker-3:9092', eps: 5.0, status: 'OPTIMAL', progressPct: 99.7 },
      { partitionId: 5, topic: 'ad-click-events', logEndOffset: 184510, committedOffset: 184507, lag: 3, assignedSlot: 'TaskManager-03 [Slot 1]', brokerNode: 'broker-3:9092', eps: 4.9, status: 'OPTIMAL', progressPct: 99.8 },
    ];

    const initialTotalLEO = initialPartitions.reduce((acc, p) => acc + p.logEndOffset, 0);
    const initialTotalCommitted = initialPartitions.reduce((acc, p) => acc + p.committedOffset, 0);

    this.kafkaState = {
      consumerGroup: 'flink-click-fraud-pipeline',
      topic: 'ad-click-events',
      totalLogEndOffset: initialTotalLEO,
      totalCommittedOffset: initialTotalCommitted,
      totalLag: initialTotalLEO - initialTotalCommitted,
      avgFetchLatencyMs: 4.2,
      producerRateEps: 29.8,
      consumerRateEps: 29.7,
      backpressureRatio: 0.02,
      checkpointId: 248,
      lastCheckpointDurationMs: 24,
      partitions: initialPartitions,
      lagHistory: [
        { time: '10:00', totalLag: 16, producerEps: 28, consumerEps: 28 },
        { time: '10:01', totalLag: 18, producerEps: 30, consumerEps: 30 },
        { time: '10:02', totalLag: 15, producerEps: 29, consumerEps: 29 },
        { time: '10:03', totalLag: 19, producerEps: 31, consumerEps: 30 },
      ]
    };
  }

  public subscribe(fn: (event: AdClickEvent, engineState: FlinkEngineState, kafkaState: KafkaConsumerLagState) => void) {
    this.listeners.push(fn);
    return () => {
      this.listeners = this.listeners.filter(l => l !== fn);
    };
  }

  public updateConfig(newConfig: Partial<FlinkEngineState>) {
    this.state = { ...this.state, ...newConfig };
  }

  public getState(): FlinkEngineState {
    return { ...this.state };
  }

  public getKafkaLagState(): KafkaConsumerLagState {
    return { ...this.kafkaState };
  }

  public start() {
    if (this.timer) return;
    this.isRunning = true;
    this.scheduleNext();
  }

  public stop() {
    if (this.timer) {
      window.clearTimeout(this.timer);
      this.timer = null;
    }
    this.isRunning = false;
  }

  public setSpeed(multiplier: number) {
    if (multiplier <= 0) return;
    this.speedMs = Math.max(120, Math.floor(850 / multiplier));
  }

  public injectAttack(fraudType: FraudType, count: number = 10) {
    for (let i = 0; i < count; i++) {
      this.activeAttackQueue.push(fraudType);
    }
    if (!this.isRunning) {
      this.generateAndProcess();
    }
  }

  public stepOnce() {
    this.generateAndProcess();
  }

  private scheduleNext() {
    if (!this.isRunning) return;
    const jitter = Math.floor(Math.random() * 120) - 60;
    const delay = Math.max(80, this.speedMs + jitter);
    this.timer = window.setTimeout(() => {
      this.generateAndProcess();
      this.scheduleNext();
    }, delay);
  }

  private generateAndProcess() {
    const forcedAttack = this.activeAttackQueue.shift() || null;
    const now = Date.now();
    eventSequence += 1;
    const eventId = `EVT${eventSequence}`;
    const clickId = `CLK_${Math.random().toString(36).substring(2, 9).toUpperCase()}`;
    const isoTimestamp = new Date(now).toISOString();

    const campaign = CAMPAIGNS[Math.floor(Math.random() * CAMPAIGNS.length)];
    let publisher = PUBLISHERS[Math.floor(Math.random() * PUBLISHERS.length)];
    let net = RESIDENTIAL_NETWORKS[Math.floor(Math.random() * RESIDENTIAL_NETWORKS.length)];
    let device = DEVICES[Math.floor(Math.random() * (DEVICES.length - 1))];
    let userId = `U${Math.floor(1000 + Math.random() * 8000)}`;
    let ip = `${net.ipPrefix}.${Math.floor(Math.random() * 254)}.${Math.floor(Math.random() * 250) + 1}`;
    let clickPosition = ['banner', 'interstitial', 'native', 'footer'][Math.floor(Math.random() * 4)];
    let referrer = ['google.com', 'facebook.com', 'twitter.com', 'instagram.com', 'direct'][Math.floor(Math.random() * 5)];
    let coords = { x: Math.floor(40 + Math.random() * 320), y: Math.floor(100 + Math.random() * 600) };
    let touchPressure = parseFloat((0.65 + Math.random() * 0.35).toFixed(2));
    let timeSinceImpression = Math.floor(1500 + Math.random() * 35000);
    let ctitMs = Math.floor(8000 + Math.random() * 45000);
    let isGeoAnomaly = false;

    // Detect if this event should be fraudulent
    const isAttack = forcedAttack || (publisher.trust < 0.4 && Math.random() < 0.7) || (Math.random() < 0.14);
    let targetFraudType: FraudType = 'NONE';

    if (isAttack) {
      targetFraudType = forcedAttack || (
        publisher.trust < 0.3 ? 'DATACENTER_BOT' :
        Math.random() < 0.35 ? 'CLICK_SPAMMING' :
        Math.random() < 0.65 ? 'CLICK_INJECTION' :
        Math.random() < 0.85 ? 'GHOST_CLICK' : 'COORDINATED_FARM'
      );

      if (targetFraudType === 'CLICK_SPAMMING') {
        ip = '103.25.12.45'; // Fixed IP generating high-speed burst
        userId = 'U1023';
        device = DEVICES[2]; // DEV1023
        coords = { x: 210, y: 350 };
      } else if (targetFraudType === 'CLICK_INJECTION') {
        publisher = PUBLISHERS[3];
        ctitMs = Math.floor(180 + Math.random() * 700); // CTIT < 1.8s
      } else if (targetFraudType === 'DATACENTER_BOT') {
        net = DATACENTER_NETWORKS[Math.floor(Math.random() * DATACENTER_NETWORKS.length)];
        ip = `${net.ipPrefix}.${Math.floor(Math.random() * 254)}.${Math.floor(Math.random() * 250) + 1}`;
        device = DEVICES[4]; // Headless Chrome
        touchPressure = 0.0;
        coords = { x: 0, y: 0 };
      } else if (targetFraudType === 'GHOST_CLICK') {
        timeSinceImpression = -1; // Unmatched impression in Interval Join
      } else if (targetFraudType === 'COORDINATED_FARM') {
        publisher = PUBLISHERS[4];
        coords = { x: 180, y: 320 }; // Identical screen coordinates across devices
        touchPressure = 1.0;
      }
    }

    // ==========================================
    // FLINK WINDOW FEATURE EXTRACTION (10 FEATURES)
    // ==========================================
    const windowCutoff = now - 60000; // 1-minute tumbling/sliding window

    // Update IP State Store
    let ipState = ipStateStore.get(ip);
    if (!ipState) {
      ipState = {
        timestamps: [],
        users: new Set(),
        devices: new Set(),
        ads: new Set(),
        lastAd: campaign.adId,
        adCounts: {},
        lastCountry: net.country
      };
      ipStateStore.set(ip, ipState);
    }
    ipState.timestamps = ipState.timestamps.filter(t => t > windowCutoff);
    ipState.timestamps.push(now);
    ipState.users.add(userId);
    ipState.devices.add(device.id);
    ipState.ads.add(campaign.adId);
    ipState.adCounts[campaign.adId] = (ipState.adCounts[campaign.adId] || 0) + 1;

    // Geographic check
    if (ipState.lastCountry !== net.country && ipState.timestamps.length > 1) {
      isGeoAnomaly = true;
    }
    ipState.lastCountry = net.country;

    // Update User State Store
    let userState = userStateStore.get(userId);
    if (!userState) {
      userState = {
        timestamps: [],
        users: new Set([userId]),
        devices: new Set([device.id]),
        ads: new Set(),
        lastAd: campaign.adId,
        adCounts: {},
        lastCountry: net.country
      };
      userStateStore.set(userId, userState);
    }
    userState.timestamps = userState.timestamps.filter(t => t > windowCutoff);
    userState.timestamps.push(now);
    userState.ads.add(campaign.adId);
    userState.adCounts[campaign.adId] = (userState.adCounts[campaign.adId] || 0) + 1;

    // Compute Feature 6: Time since previous click from same IP
    let timeBetweenClicksMs = 9999;
    if (ipState.timestamps.length >= 2) {
      timeBetweenClicksMs = ipState.timestamps[ipState.timestamps.length - 1] - ipState.timestamps[ipState.timestamps.length - 2];
    }

    // Compute Feature 10: Bot behavior score (0 - 100)
    let botScore = 10;
    if (timeBetweenClicksMs < 800) botScore += 35;
    if (ipState.timestamps.length > 20) botScore += 25;
    if (net.asn.includes('Datacenter') || net.asn.includes('AWS')) botScore += 30;
    if (touchPressure === 0.0) botScore += 20;
    botScore = Math.min(100, botScore);

    const features: BehavioralFeatures = {
      clicksPerMinuteUser: userState.timestamps.length,
      clicksFromSameIp: ipState.timestamps.length,
      clicksPerDevice: Math.max(1, Math.floor(ipState.timestamps.length * 0.8)),
      uniqueUsersPerIp: ipState.users.size,
      uniqueDevicesPerIp: ipState.devices.size,
      timeSincePreviousClickMs: timeBetweenClicksMs,
      sameAdClickCount: ipState.adCounts[campaign.adId] || 1,
      uniqueAdsClicked: userState.ads.size,
      isGeoAnomaly: isGeoAnomaly,
      botBehaviorScore: botScore
    };

    // ==========================================
    // HYBRID FRAUD DECISION ENGINE (RULES + ML)
    // ==========================================
    let ruleScore = 0;
    const reasons: string[] = [];

    // Rule 1: Click Frequency
    if (features.clicksPerMinuteUser > 20 || features.clicksFromSameIp > 25) {
      ruleScore += 25;
      reasons.push("High click frequency (>20 clicks/min)");
    }

    // Rule 2: Rapid Clicking / Time Between Clicks
    if (features.timeSincePreviousClickMs < 1000) {
      ruleScore += 25;
      reasons.push(`Rapid clicking (${features.timeSincePreviousClickMs}ms interval < 1.0s)`);
    }

    // Rule 3: Same IP Repetition
    if (features.clicksFromSameIp > 15) {
      ruleScore += 20;
      reasons.push(`Repeated clicks from same IP (${features.clicksFromSameIp} in window)`);
    }

    // Rule 4: Same Ad Repetition
    if (features.sameAdClickCount > 8) {
      ruleScore += 15;
      reasons.push(`Repeated advertisement clicks (${features.sameAdClickCount} times)`);
    }

    // Rule 5: Multiple users / devices per IP
    if (features.uniqueUsersPerIp > 4 || features.uniqueDevicesPerIp > 4) {
      ruleScore += 15;
      reasons.push(`Multiple accounts/devices per IP (${features.uniqueUsersPerIp} users, ${features.uniqueDevicesPerIp} devices)`);
    }

    // Additional Specialized Flink CEP & Network Rules
    if (net.asn.includes('AWS') || net.asn.includes('DigitalOcean') || net.asn.includes('Hetzner')) {
      ruleScore += 30;
      reasons.push("Cloud datacenter / proxy hosting ASN detected");
    }
    if (ctitMs < this.state.minAllowedCtitMs) {
      ruleScore += 35;
      reasons.push(`Click Injection (CTIT: ${ctitMs}ms < ${this.state.minAllowedCtitMs}ms)`);
    }
    if (timeSinceImpression < 0) {
      ruleScore += 30;
      reasons.push("Ghost click: No valid prior impression in Interval Join");
    }

    ruleScore = Math.min(100, ruleScore);

    // Simulated XGBoost / Random Forest Inference on TalkingData features
    let mlLogits = -2.8; // Baseline low fraud probability for normal traffic
    if (features.clicksFromSameIp > 10) mlLogits += 2.1;
    if (features.timeSincePreviousClickMs < 1000) mlLogits += 2.4;
    if (features.sameAdClickCount > 5) mlLogits += 1.3;
    if (net.asn.includes('AWS') || net.asn.includes('Datacenter')) mlLogits += 2.9;
    if (ctitMs < 1800) mlLogits += 3.2;
    if (targetFraudType !== 'NONE') mlLogits += 1.8;

    const mlProbability = parseFloat((1 / (1 + Math.exp(-mlLogits))).toFixed(3));

    // Hybrid Formula: Risk Score = (Rule Score * 0.4) + (ML Probability * 100 * 0.6)
    const combinedRisk = Math.min(100, Math.round((ruleScore * this.state.ruleWeight) + (mlProbability * 100 * this.state.mlWeight)));
    
    // Risk Level Thresholds: 0-30 Low, 31-60 Medium, 61-100 High
    const riskLevel: RiskLevel = combinedRisk >= 61 ? 'HIGH' : combinedRisk >= 31 ? 'MEDIUM' : 'LOW';
    const action: ActionStatus = riskLevel === 'HIGH' ? 'BLOCKED' : riskLevel === 'MEDIUM' ? 'FLAGGED' : 'VALID';

    // Map detected type
    let finalDetectedType = targetFraudType;
    if (finalDetectedType === 'NONE' && riskLevel === 'HIGH') {
      finalDetectedType = features.clicksFromSameIp > 15 ? 'CLICK_SPAMMING' : 'COORDINATED_FARM';
    }

    // Update Flink Engine Metrics
    const isFraudulent = riskLevel === 'HIGH' || riskLevel === 'MEDIUM';
    this.state.totalIngested += 1;
    if (isFraudulent) {
      this.state.totalFraud += 1;
      this.state.totalBudgetProtected += campaign.cpc;
    } else {
      this.state.totalValid += 1;
    }
    this.state.activeRocksDbKeys = ipStateStore.size + userStateStore.size + 1500;
    this.state.watermarkDelayMs = Math.floor(18 + Math.random() * 22);

    const event: AdClickEvent = {
      eventId: eventId,
      clickId: clickId,
      timestamp: isoTimestamp,
      epochMs: now,
      userId: userId,
      adId: campaign.adId,
      campaignId: campaign.id,
      campaignName: campaign.name,
      publisherId: publisher.id,
      publisherName: publisher.name,
      costPerClick: campaign.cpc,

      appId: campaign.appId,
      channelId: campaign.channelId,
      deviceType: device.type,
      osVersion: device.os,
      browser: device.browser,

      ipAddress: ip,
      asn: net.asn,
      country: net.country,
      city: net.city,
      clickPosition: clickPosition,
      referrer: referrer,
      clickCoordinates: coords,
      touchPressure: touchPressure,
      timeSinceImpressionMs: timeSinceImpression,
      clickToInstallTimeMs: ctitMs,

      features: features,

      ruleScore: ruleScore,
      mlProbability: mlProbability,
      riskScore: combinedRisk,
      riskLevel: riskLevel,
      action: action,
      detectedFraudType: finalDetectedType,
      reasons: reasons.length > 0 ? reasons : ['Verified authentic user interaction'],
      processingLatencyMs: Math.floor(12 + Math.random() * 24)
    };

    // Update Kafka Partition & Offset Telemetry
    const partitionIndex = Math.abs((publisher.id + ip).split('').reduce((acc, c) => acc + c.charCodeAt(0), 0)) % 6;
    const recordsInStep = forcedAttack ? Math.floor(6 + Math.random() * 8) : 1;

    // Simulate Kafka broker LEO update
    this.kafkaState.partitions[partitionIndex].logEndOffset += recordsInStep;

    // Flink consumer continuously processes and commits
    this.kafkaState.partitions.forEach((p, idx) => {
      // Natural jitter for consumption
      const consumeCount = idx === partitionIndex 
        ? Math.min(p.logEndOffset - p.committedOffset, recordsInStep + (p.lag > 3 ? 1 : 0))
        : (p.lag > 0 ? 1 : 0);
      
      p.committedOffset += consumeCount;
      p.lag = Math.max(0, p.logEndOffset - p.committedOffset);
      p.status = p.lag > 25 ? 'HIGH_LAG' : p.lag > 10 ? 'ELEVATED' : 'OPTIMAL';
      p.progressPct = parseFloat(((p.committedOffset / p.logEndOffset) * 100).toFixed(2));
      p.eps = parseFloat((4.5 + Math.random() * 1.5).toFixed(1));
    });

    // Checkpoint coordination (Chandy-Lamport snapshot every ~10 events)
    if (this.state.totalIngested % 8 === 0) {
      this.checkpointCounter += 1;
      this.kafkaState.checkpointId = this.checkpointCounter;
      this.kafkaState.lastCheckpointDurationMs = Math.floor(16 + Math.random() * 18);
    }

    this.kafkaState.totalLogEndOffset = this.kafkaState.partitions.reduce((acc, p) => acc + p.logEndOffset, 0);
    this.kafkaState.totalCommittedOffset = this.kafkaState.partitions.reduce((acc, p) => acc + p.committedOffset, 0);
    this.kafkaState.totalLag = this.kafkaState.totalLogEndOffset - this.kafkaState.totalCommittedOffset;
    this.kafkaState.backpressureRatio = this.kafkaState.totalLag > 30 ? 0.22 : this.kafkaState.totalLag > 15 ? 0.08 : 0.02;
    this.kafkaState.avgFetchLatencyMs = parseFloat((3.8 + Math.random() * 1.4).toFixed(1));

    // Rolling Lag History for sparklines
    const timeStr = new Date(now).toLocaleTimeString().split(' ')[0];
    if (this.kafkaState.lagHistory.length === 0 || this.kafkaState.lagHistory[this.kafkaState.lagHistory.length - 1].time !== timeStr) {
      this.kafkaState.lagHistory.push({
        time: timeStr,
        totalLag: this.kafkaState.totalLag,
        producerEps: Math.floor(26 + Math.random() * 8),
        consumerEps: Math.floor(25 + Math.random() * 8)
      });
      if (this.kafkaState.lagHistory.length > 15) {
        this.kafkaState.lagHistory.shift();
      }
    }

    this.listeners.forEach(fn => fn(event, this.state, this.kafkaState));
  }
}

export const globalFlinkStream = new FlinkStreamEngine();
