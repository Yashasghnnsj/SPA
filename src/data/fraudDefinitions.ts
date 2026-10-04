import { FraudPatternDefinition } from '../types/fraud';

export const FRAUD_DEFINITIONS: FraudPatternDefinition[] = [
  {
    id: 'CLICK_SPAMMING',
    name: 'Click Spamming / Flooding',
    severity: 'HIGH',
    description: 'High frequency click bursts generated from single IP or Device ID within seconds to siphon pay-per-click budget or flood attribution networks.',
    industryImpact: 'Costs advertisers $14B annually in wasted CPC budgets without genuine human impressions.',
    flinkMechanism: 'KeyedStream by (IP + PublisherId) with a 10-second Sliding Event-Time Window sliding every 1s. Trigger alert if click_count > threshold (e.g., >8 clicks / 10s).',
    windowType: 'SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1))',
    sampleIndicator: 'Click frequency > 12 clicks in 6 seconds from same IP address'
  },
  {
    id: 'CLICK_INJECTION',
    name: 'Click Injection (Install Hijacking)',
    severity: 'CRITICAL',
    description: 'Mobile malware on Android detects the BROADCAST_PACKAGE_ADDED intent when a user downloads an app, immediately firing a synthetic click before app launch to claim attribution bounty.',
    industryImpact: 'Steals Cost-Per-Install (CPI) payouts from legitimate organic channels or actual publisher partners.',
    flinkMechanism: 'Flink CEP (Complex Event Processing) pattern: MATCHING Pattern.begin("click").followedBy("install").where(eventTimeDiff < 2000ms). Clicks occurring <2s before installation are flagged as physical impossibilities.',
    windowType: 'CEP Pattern with within(Time.seconds(10)) and KeyedProcessFunction',
    sampleIndicator: 'Click-to-Install Time (CTIT) < 850ms (Humanly impossible download + install window)'
  },
  {
    id: 'DATACENTER_BOT',
    name: 'Datacenter Botnet / Headless Crawler',
    severity: 'CRITICAL',
    description: 'Automated Puppeteer/Selenium headless browsers hosted in cloud datacenters (AWS, DigitalOcean, Hetzner) generating artificial engagement.',
    industryImpact: 'Infiltrates programmatic ad exchanges, skewing CTR metrics and burning enterprise programmatic budgets.',
    flinkMechanism: 'Enrichment Stream using AsyncIO or BroadcastState joined with MaxMind GeoIP and Autonomous System Number (ASN) datacenter blacklist database. Zero human touch pressure.',
    windowType: 'BroadcastProcessFunction / AsyncDataStream with RocksDB cached lookups',
    sampleIndicator: 'ASN 16509 (Amazon AWS) or ASN 14061 (DigitalOcean) originating mobile in-app clicks'
  },
  {
    id: 'GHOST_CLICK',
    name: 'Ghost Click (Click Without Impression)',
    severity: 'HIGH',
    description: 'Synthetic clicks generated directly against conversion or tracking endpoints without any corresponding ad banner impression ever served.',
    industryImpact: 'Direct indication of API replay attacks, compromised ad tags, or unauthorized scraping.',
    flinkMechanism: 'Dual-Stream Interval Join between raw-impressions and raw-clicks streams: clicksStream.keyBy(impressionToken).intervalJoin(impressionsStream.keyBy(impressionToken)).between(Time.seconds(-60), Time.seconds(0)). Unmatched clicks routed to SideOutput.',
    windowType: 'KeyedStream.intervalJoin().between(lowerBound, upperBound)',
    sampleIndicator: 'Click event has no valid token matching an impression served in the preceding 60 seconds'
  },
  {
    id: 'COORDINATED_FARM',
    name: 'Coordinated Device Farm / Precision Emulators',
    severity: 'MEDIUM',
    description: 'Emulators or click farms clicking identical pixel coordinates (e.g., exact center x:180, y:320) with near-zero coordinate variance and invariant hardware fingerprints.',
    industryImpact: 'Massive volume fraud originating from offshore click farms using rotating residential proxies.',
    flinkMechanism: 'Stateful KeyedProcessFunction tracking running Shannon entropy of touch coordinates and inter-click time intervals per publisher ad unit. Entropy < 1.2 triggers bot farm classification.',
    windowType: 'TumblingEventTimeWindows.of(Time.minutes(1)) with AggregateFunction<Click, VarianceState>',
    sampleIndicator: 'Screen touch coordinates identical across 20+ distinct sessions with 0px variance'
  }
];
