import React, { useState } from 'react';
import { FRAUD_DEFINITIONS } from '../data/fraudDefinitions';
import { FraudType, FraudPatternDefinition } from '../types/fraud';
import { globalFlinkStream } from '../services/streamSimulator';
import { 
  Flame, 
  ShieldAlert, 
  Play, 
  CheckCircle2, 
  Layers, 
  AlertTriangle, 
  Terminal, 
  Zap,
  ArrowRight
} from 'lucide-react';

interface AttackLabViewProps {
  onAttackLaunched: () => void;
}

export const AttackLabView: React.FC<AttackLabViewProps> = ({ onAttackLaunched }) => {
  const [selectedFraudId, setSelectedFraudId] = useState<FraudType>('CLICK_SPAMMING');
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  const selectedFraud = FRAUD_DEFINITIONS.find(f => f.id === selectedFraudId) || FRAUD_DEFINITIONS[0];

  const handleLaunchAttack = (id: FraudType) => {
    globalFlinkStream.injectAttack(id, 10);
    setStatusMessage(`Dispatched 10 simulated [${id}] attack events to the Apache Flink ingestion pipeline!`);
    onAttackLaunched();
    setTimeout(() => setStatusMessage(null), 4000);
  };

  return (
    <div className="space-y-6">

      {/* Header Banner */}
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl relative overflow-hidden">
        <div className="absolute right-0 top-0 w-80 h-full bg-gradient-to-l from-rose-500/10 to-transparent pointer-events-none" />
        <div className="max-w-3xl space-y-2">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-rose-400">
            <Flame className="w-4 h-4" />
            <span>Interactive Attack Simulation & Flink Defense Laboratory</span>
          </div>
          <h2 className="text-xl font-bold text-white">
            Ad Tech Fraud Vectors & Stream Processing Countermeasures
          </h2>
          <p className="text-xs text-slate-300 leading-relaxed">
            Test each fraud vector against Apache Flink stateful operators. Launch simulated attack waves into 
            the live Kafka pipeline to verify sliding window counters, CEP pattern automata, and interval join rules.
          </p>
        </div>
      </div>

      {statusMessage && (
        <div className="p-3 bg-cyan-950/40 border border-cyan-500/40 rounded-lg text-xs text-cyan-300 flex items-center justify-between animate-in fade-in duration-200">
          <span>{statusMessage}</span>
          <span className="font-semibold text-cyan-400">Check Real-Time NOC tab for live log</span>
        </div>
      )}

      {/* 2-Column Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

        {/* Left Column: List of Fraud Vectors */}
        <div className="space-y-3">
          <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider px-1 block">
            Select Fraud Vector:
          </span>
          {FRAUD_DEFINITIONS.map((fraud) => {
            const isSelected = fraud.id === selectedFraudId;
            return (
              <button
                key={fraud.id}
                onClick={() => setSelectedFraudId(fraud.id)}
                className={`w-full text-left p-4 rounded-xl border transition-all flex flex-col justify-between ${
                  isSelected
                    ? 'bg-slate-800/90 border-cyan-400/80 shadow-lg shadow-cyan-950/40'
                    : 'bg-slate-900/60 border-slate-800 hover:border-slate-700 hover:bg-slate-800/40'
                }`}
              >
                <div className="flex items-center justify-between text-xs mb-1">
                  <span className={`font-semibold ${isSelected ? 'text-cyan-300' : 'text-white'}`}>
                    {fraud.name}
                  </span>
                  <span className={`text-[10px] font-mono font-bold ${
                    fraud.severity === 'CRITICAL' ? 'text-rose-400' :
                    fraud.severity === 'HIGH' ? 'text-amber-400' : 'text-slate-400'
                  }`}>
                    {fraud.severity}
                  </span>
                </div>
                <p className="text-[11px] text-slate-400 line-clamp-2 mt-1">
                  {fraud.description}
                </p>
              </button>
            );
          })}
        </div>

        {/* Right 2 Columns: Deep Vector Inspection & Launch Pad */}
        <div className="lg:col-span-2 space-y-6">
          <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-6">
            
            {/* Vector Title & Launch Action */}
            <div className="flex flex-wrap items-center justify-between gap-4 border-b border-slate-800 pb-4">
              <div>
                <span className="text-[11px] font-mono text-cyan-400 uppercase font-semibold">
                  Vector Signature: {selectedFraud.id}
                </span>
                <h3 className="text-lg font-bold text-white mt-0.5">
                  {selectedFraud.name}
                </h3>
              </div>

              <button
                onClick={() => handleLaunchAttack(selectedFraud.id)}
                className="px-4 py-2 text-xs font-semibold text-slate-950 bg-rose-400 hover:bg-rose-300 rounded-lg transition-colors flex items-center gap-1.5 shadow-md shadow-rose-950"
              >
                <Flame className="w-3.5 h-3.5" />
                <span>Simulate Attack Wave</span>
              </button>
            </div>

            {/* Attack Anatomy */}
            <div className="space-y-4">
              <div>
                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  1. Attack Vector Anatomy & Threat Model
                </h4>
                <p className="text-xs text-slate-200 leading-relaxed mt-1.5">
                  {selectedFraud.description}
                </p>
              </div>

              <div className="p-4 bg-slate-950 border border-slate-800 rounded-lg">
                <span className="text-[11px] font-semibold text-rose-400 uppercase tracking-wider block mb-1">
                  Financial & Industry Impact:
                </span>
                <p className="text-xs text-slate-300">
                  {selectedFraud.industryImpact}
                </p>
              </div>
            </div>

            {/* Flink Defense Operator Mechanism */}
            <div className="space-y-3 pt-4 border-t border-slate-800">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
                <Zap className="w-3.5 h-3.5 text-cyan-400" />
                2. Apache Flink Stream Operator Countermeasure
              </h4>
              <p className="text-xs text-slate-200 leading-relaxed">
                {selectedFraud.flinkMechanism}
              </p>

              <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg text-xs font-mono space-y-1.5">
                <div className="text-slate-400">
                  Flink Window / State Operator: <span className="text-cyan-300">{selectedFraud.windowType}</span>
                </div>
                <div className="text-slate-400">
                  Detection Indicator: <span className="text-amber-300">{selectedFraud.sampleIndicator}</span>
                </div>
              </div>
            </div>

            {/* Architecture Comparison: Batch vs Flink */}
            <div className="pt-4 border-t border-slate-800">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
                3. Batch (Hadoop/Spark) vs Flink Real-Time Comparison
              </h4>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
                <div className="p-3 bg-rose-950/20 border border-rose-800/40 rounded-lg space-y-1">
                  <span className="font-semibold text-rose-300">Nightly Batch Processing</span>
                  <p className="text-slate-400 text-[11px] leading-relaxed">
                    Evaluated 24 hours later. By then, budget is drained, CPI bounty is disbursed to fraudulent affiliates, and damage is irrecoverable.
                  </p>
                </div>
                <div className="p-3 bg-emerald-950/20 border border-emerald-800/40 rounded-lg space-y-1">
                  <span className="font-semibold text-emerald-300">Apache Flink Stream Pipeline</span>
                  <p className="text-slate-400 text-[11px] leading-relaxed">
                    Evaluated in-flight within 25ms. Unmatched or spam clicks are diverted to side output before billing reconciliation executes.
                  </p>
                </div>
              </div>
            </div>

          </div>
        </div>

      </div>

    </div>
  );
};
