import React, { useState } from 'react';
import { X, Copy, Check, ShieldAlert, CheckCircle2, AlertTriangle, Cpu, Globe, Crosshair, HelpCircle, Activity } from 'lucide-react';
import { AdClickEvent } from '../types/fraud';

interface ClickDetailModalProps {
  click: AdClickEvent | null;
  onClose: () => void;
}

export const ClickDetailModal: React.FC<ClickDetailModalProps> = ({ click, onClose }) => {
  const [copied, setCopied] = useState(false);

  if (!click) return null;

  const handleCopyJson = () => {
    navigator.clipboard.writeText(JSON.stringify(click, null, 2));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const isHighRisk = click.riskLevel === 'HIGH';
  const isMedRisk = click.riskLevel === 'MEDIUM';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-in fade-in duration-200">
      <div 
        className="w-full max-w-4xl max-h-[92vh] overflow-y-auto bg-slate-900 border border-slate-800 rounded-xl shadow-2xl flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Modal Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-950/60 sticky top-0 z-10">
          <div className="flex items-center gap-3">
            <div className={`w-9 h-9 rounded-lg flex items-center justify-center ${
              isHighRisk ? 'bg-rose-500/10 text-rose-400 border border-rose-500/30' :
              isMedRisk ? 'bg-amber-500/10 text-amber-400 border border-amber-500/30' :
              'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30'
            }`}>
              {isHighRisk ? <ShieldAlert className="w-5 h-5" /> :
               isMedRisk ? <AlertTriangle className="w-5 h-5" /> :
               <CheckCircle2 className="w-5 h-5" />}
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-base font-semibold text-white font-mono">{click.eventId}</h3>
                <span className="text-xs text-slate-400 font-mono">({click.clickId})</span>
                <span className={`text-xs font-bold font-mono px-2 py-0.5 rounded ${
                  isHighRisk ? 'bg-rose-950 text-rose-300 border border-rose-800' :
                  isMedRisk ? 'bg-amber-950 text-amber-300 border border-amber-800' :
                  'bg-emerald-950 text-emerald-300 border border-emerald-800'
                }`}>
                  {click.riskLevel} RISK · {click.action}
                </span>
              </div>
              <p className="text-xs text-slate-400">
                Timestamp: {click.timestamp} · Stream Processing Latency: <strong className="text-cyan-400 font-mono">{click.processingLatencyMs}ms</strong>
              </p>
            </div>
          </div>

          <button
            onClick={onClose}
            className="p-1.5 text-slate-400 hover:text-white rounded-lg hover:bg-slate-800 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Modal Body */}
        <div className="p-6 space-y-6">

          {/* Section: The 8 Core Project Questions Answered */}
          <div className="p-4 bg-slate-950/80 border border-cyan-500/30 rounded-xl space-y-3">
            <div className="flex items-center justify-between border-b border-slate-800 pb-2">
              <span className="text-xs font-bold text-cyan-400 uppercase tracking-wider flex items-center gap-1.5">
                <HelpCircle className="w-3.5 h-3.5" />
                The 8 Core Project Questions Answered
              </span>
              <span className="text-[11px] text-slate-400 font-mono">Real-Time Evaluation</span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">1. Who clicked the advertisement?</span>
                <div className="text-white font-medium mt-0.5">
                  User ID: <span className="font-mono text-cyan-300">{click.userId}</span> · Device: <span className="font-mono">{click.deviceType}</span> ({click.browser}, {click.osVersion})
                </div>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">2. Which advertisement was clicked?</span>
                <div className="text-white font-medium mt-0.5">
                  Ad ID: <span className="font-mono text-cyan-300">{click.adId}</span> · Campaign: <span className="text-slate-200">{click.campaignName}</span> (App: {click.appId})
                </div>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">3. When did the click happen?</span>
                <div className="text-white font-medium mt-0.5 font-mono">
                  {click.timestamp}
                </div>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">4. From which IP/device/location?</span>
                <div className="text-white font-medium mt-0.5">
                  IP: <span className="font-mono text-cyan-300">{click.ipAddress}</span> ({click.city}, {click.country}) · ASN: {click.asn}
                </div>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">5. Is the click suspicious?</span>
                <div className="text-white font-medium mt-0.5 flex items-center gap-2">
                  <span className={`font-bold font-mono ${isHighRisk ? 'text-rose-400' : isMedRisk ? 'text-amber-400' : 'text-emerald-400'}`}>
                    {isHighRisk ? 'YES (High Suspicion)' : isMedRisk ? 'MONITOR (Medium Suspicion)' : 'NO (Legitimate)'}
                  </span>
                  <span>·</span>
                  <span className="text-slate-400">Final Risk: <strong>{click.riskScore}/100</strong></span>
                </div>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">6. Why was it considered suspicious?</span>
                <ul className="text-rose-300 font-medium mt-0.5 list-disc list-inside">
                  {click.reasons.map((r, i) => (
                    <li key={i}>{r}</li>
                  ))}
                </ul>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">7. How many clicks are occurring?</span>
                <div className="text-white font-medium mt-0.5">
                  From User: <span className="font-mono text-cyan-300">{click.features.clicksPerMinuteUser}</span>/min · From IP: <span className="font-mono text-cyan-300">{click.features.clicksFromSameIp}</span>/min
                </div>
              </div>

              <div className="p-2.5 bg-slate-900/60 rounded-lg border border-slate-800">
                <span className="text-slate-400 block text-[11px]">8. Can it be detected within seconds?</span>
                <div className="text-emerald-400 font-medium mt-0.5 font-mono">
                  YES: Detected in {click.processingLatencyMs}ms (Sub-100ms Apache Flink SLA)
                </div>
              </div>
            </div>
          </div>

          {/* Section: Hybrid Scoring Formula Breakdown */}
          <div className="p-4 bg-slate-950/60 border border-slate-800 rounded-xl space-y-3">
            <span className="text-xs font-semibold text-slate-300 uppercase tracking-wider block">
              Hybrid Scoring Engine Breakdown: Rules + ML Model
            </span>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
              <div className="p-3 bg-slate-900 border border-slate-800 rounded-lg">
                <span className="text-slate-400 text-[11px]">Rule-Based Score (0-100):</span>
                <div className="text-lg font-bold font-mono text-amber-300 mt-1">{click.ruleScore} / 100</div>
                <p className="text-[11px] text-slate-500 mt-1">Weight: 40% of final score</p>
              </div>

              <div className="p-3 bg-slate-900 border border-slate-800 rounded-lg">
                <span className="text-slate-400 text-[11px]">ML Model Probability (XGBoost):</span>
                <div className="text-lg font-bold font-mono text-purple-300 mt-1">{click.mlProbability} ({(click.mlProbability * 100).toFixed(1)}%)</div>
                <p className="text-[11px] text-slate-500 mt-1">Weight: 60% of final score</p>
              </div>

              <div className="p-3 bg-slate-900 border border-slate-800 rounded-lg">
                <span className="text-slate-400 text-[11px]">Final Risk Score (0-100):</span>
                <div className={`text-lg font-bold font-mono mt-1 ${isHighRisk ? 'text-rose-400' : isMedRisk ? 'text-amber-400' : 'text-emerald-400'}`}>
                  {click.riskScore} / 100 ({click.riskLevel})
                </div>
                <p className="text-[11px] text-slate-500 mt-1">Formula: (Rule * 0.4) + (ML * 60)</p>
              </div>
            </div>
          </div>

          {/* Section: The 10 Real-Time Behavioral Features */}
          <div className="p-4 bg-slate-950/60 border border-slate-800 rounded-xl space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-xs font-semibold text-slate-300 uppercase tracking-wider">
                The 10 Behavioral Streaming Features (Derived in Flink Windows)
              </span>
              <span className="text-[11px] text-slate-500 font-mono">TalkingData + Flink State</span>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F1. Clicks/Min (User)</span>
                <span className="font-mono text-white font-bold">{click.features.clicksPerMinuteUser}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F2. IP Click Freq</span>
                <span className="font-mono text-white font-bold">{click.features.clicksFromSameIp}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F3. Device Click Freq</span>
                <span className="font-mono text-white font-bold">{click.features.clicksPerDevice}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F4. Unique Users/IP</span>
                <span className="font-mono text-white font-bold">{click.features.uniqueUsersPerIp}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F5. Unique Devices/IP</span>
                <span className="font-mono text-white font-bold">{click.features.uniqueDevicesPerIp}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F6. Time Between Clicks</span>
                <span className="font-mono text-white font-bold">{click.features.timeSincePreviousClickMs}ms</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F7. Same Ad Clicks</span>
                <span className="font-mono text-white font-bold">{click.features.sameAdClickCount}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F8. Unique Ads Clicked</span>
                <span className="font-mono text-white font-bold">{click.features.uniqueAdsClicked}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F9. Geo Anomaly</span>
                <span className="font-mono text-white font-bold">{click.features.isGeoAnomaly ? 'TRUE (Anomaly)' : 'FALSE'}</span>
              </div>
              <div className="p-2 bg-slate-900 border border-slate-800 rounded">
                <span className="text-[10px] text-slate-500 block">F10. Bot Behavior Score</span>
                <span className="font-mono text-white font-bold">{click.features.botBehaviorScore}/100</span>
              </div>
            </div>
          </div>

          {/* Section: Raw JSON Payload */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
                Raw Event Payload (JSON format for Kafka topic 'ad-click-events')
              </span>
              <button
                onClick={handleCopyJson}
                className="text-xs text-slate-400 hover:text-white flex items-center gap-1 transition-colors px-2.5 py-1 rounded bg-slate-800"
              >
                {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
                <span>{copied ? 'Copied' : 'Copy JSON'}</span>
              </button>
            </div>
            <pre className="p-3 bg-slate-950 border border-slate-800 rounded-lg text-xs font-mono text-cyan-300/90 overflow-x-auto max-h-44">
              {JSON.stringify(click, null, 2)}
            </pre>
          </div>

        </div>

        {/* Modal Footer */}
        <div className="px-6 py-3 border-t border-slate-800 bg-slate-950/60 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-1.5 text-xs font-medium text-slate-300 hover:text-white bg-slate-800 hover:bg-slate-700 rounded-lg transition-colors"
          >
            Close Inspector
          </button>
        </div>
      </div>
    </div>
  );
};
