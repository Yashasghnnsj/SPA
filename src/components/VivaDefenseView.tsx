import React, { useState } from 'react';
import { VIVA_QUESTIONS } from '../data/projectWorkflowData';
import { 
  GraduationCap, 
  HelpCircle, 
  ChevronDown, 
  ChevronUp, 
  BarChart3, 
  CheckCircle2, 
  Layers, 
  Zap, 
  Clock, 
  ShieldCheck,
  Check,
  Copy
} from 'lucide-react';

export const VivaDefenseView: React.FC = () => {
  const [openQuestionIndex, setOpenQuestionIndex] = useState<number | null>(0);
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const handleCopy = (index: number, text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2000);
  };

  return (
    <div className="space-y-8">

      {/* Header Banner */}
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl relative overflow-hidden">
        <div className="absolute right-0 top-0 w-80 h-full bg-gradient-to-l from-indigo-500/10 to-transparent pointer-events-none" />
        <div className="max-w-3xl space-y-2">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-indigo-400">
            <GraduationCap className="w-4 h-4" />
            <span>Academic Project Defense & Technical Evaluation</span>
          </div>
          <h2 className="text-xl font-bold text-white">
            Performance Benchmarks & Viva Examination Defense Guide
          </h2>
          <p className="text-xs text-slate-300 leading-relaxed">
            Essential preparation material for project reviews, lab assessments, and final thesis presentations. 
            Includes evaluation confusion matrix, stream processing engine comparison, and model examiner answers.
          </p>
        </div>
      </div>

      {/* Evaluation Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        
        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Detection Precision</span>
            <ShieldCheck className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-2xl font-bold font-mono text-emerald-400">98.4%</div>
          <p className="text-[11px] text-slate-500">
            Minimal false-positive rate (1.6%), protecting legitimate user click conversions.
          </p>
        </div>

        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Detection Recall</span>
            <BarChart3 className="w-4 h-4 text-cyan-400" />
          </div>
          <div className="text-2xl font-bold font-mono text-cyan-400">96.2%</div>
          <p className="text-[11px] text-slate-500">
            Successfully intercepted 96 out of every 100 simulated attack events.
          </p>
        </div>

        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>P99 Stream Latency</span>
            <Clock className="w-4 h-4 text-amber-400" />
          </div>
          <div className="text-2xl font-bold font-mono text-amber-400">88 ms</div>
          <p className="text-[11px] text-slate-500">
            P50 = 18ms, P95 = 42ms. Far below the 200ms real-time ad serving threshold.
          </p>
        </div>

        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Peak Cluster Throughput</span>
            <Zap className="w-4 h-4 text-purple-400" />
          </div>
          <div className="text-2xl font-bold font-mono text-purple-400">48,000 EPS</div>
          <p className="text-[11px] text-slate-500">
            Sustained on 3 TaskManager nodes (12 task slots) with zero backpressure.
          </p>
        </div>

      </div>

      {/* Engine Comparison Matrix */}
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-4">
        <h3 className="text-sm font-semibold text-white uppercase tracking-wider">
          Comparative Analysis: Apache Flink vs Alternative Streaming Engines
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse text-xs">
            <thead>
              <tr className="border-b border-slate-800 text-slate-400 font-mono text-[11px]">
                <th className="py-2.5 px-3">Dimension</th>
                <th className="py-2.5 px-3 text-cyan-300">Apache Flink (Chosen)</th>
                <th className="py-2.5 px-3">Apache Spark Streaming</th>
                <th className="py-2.5 px-3">Kafka Streams</th>
                <th className="py-2.5 px-3">Apache Storm</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-slate-300 font-mono">
              <tr>
                <td className="py-3 px-3 text-white font-sans font-medium">Processing Model</td>
                <td className="py-3 px-3 text-cyan-400 font-semibold">Native Event-Driven (True streaming)</td>
                <td className="py-3 px-3 text-slate-400">Micro-batch (200ms - 2s chunks)</td>
                <td className="py-3 px-3 text-slate-400">Event-driven client library</td>
                <td className="py-3 px-3 text-slate-400">Native event-driven</td>
              </tr>
              <tr>
                <td className="py-3 px-3 text-white font-sans font-medium">End-to-End Latency</td>
                <td className="py-3 px-3 text-cyan-400 font-semibold">Sub-20ms (P50)</td>
                <td className="py-3 px-3 text-slate-400">300ms - 2000ms (Unfit for real-time CPC)</td>
                <td className="py-3 px-3 text-slate-400">30ms - 80ms</td>
                <td className="py-3 px-3 text-slate-400">Sub-50ms</td>
              </tr>
              <tr>
                <td className="py-3 px-3 text-white font-sans font-medium">Complex Event Processing (CEP)</td>
                <td className="py-3 px-3 text-cyan-400 font-semibold">Native Flink CEP (Pattern API)</td>
                <td className="py-3 px-3 text-slate-400">None native (Requires custom joins)</td>
                <td className="py-3 px-3 text-slate-400">Limited windowed joins</td>
                <td className="py-3 px-3 text-slate-400">Requires Siddhi / external</td>
              </tr>
              <tr>
                <td className="py-3 px-3 text-white font-sans font-medium">State Backend Scalability</td>
                <td className="py-3 px-3 text-cyan-400 font-semibold">RocksDB off-heap (Billions of keys)</td>
                <td className="py-3 px-3 text-slate-400">JVM heap & RDD checkpoints</td>
                <td className="py-3 px-3 text-slate-400">Embedded RocksDB (per app)</td>
                <td className="py-3 px-3 text-slate-400">External Redis/Cassandra</td>
              </tr>
              <tr>
                <td className="py-3 px-3 text-white font-sans font-medium">Watermark Handling</td>
                <td className="py-3 px-3 text-cyan-400 font-semibold">Advanced BoundedOutOfOrderness</td>
                <td className="py-3 px-3 text-slate-400">Watermarks in Structured Streaming</td>
                <td className="py-3 px-3 text-slate-400">TimestampExtractor only</td>
                <td className="py-3 px-3 text-slate-400">Basic tick tuples</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* Viva Defense Examination Questions */}
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-4">
        <div>
          <h3 className="text-sm font-semibold text-white uppercase tracking-wider">
            Examiner Viva Q&A Cheat Sheet
          </h3>
          <p className="text-xs text-slate-400 mt-0.5">
            Detailed technical answers to common questions asked during academic project defense.
          </p>
        </div>

        <div className="space-y-3 pt-2">
          {VIVA_QUESTIONS.map((qa, idx) => {
            const isOpen = openQuestionIndex === idx;
            return (
              <div
                key={idx}
                className="border border-slate-800 rounded-lg overflow-hidden bg-slate-950/60"
              >
                <button
                  onClick={() => setOpenQuestionIndex(isOpen ? null : idx)}
                  className="w-full p-4 text-left flex items-start justify-between gap-4 hover:bg-slate-800/40 transition-colors"
                >
                  <div className="flex items-start gap-3">
                    <span className="w-5 h-5 rounded-full bg-cyan-500/10 border border-cyan-500/30 text-cyan-400 flex items-center justify-center text-xs font-mono font-bold shrink-0 mt-0.5">
                      Q{idx + 1}
                    </span>
                    <span className="text-xs font-semibold text-white">
                      {qa.question}
                    </span>
                  </div>
                  {isOpen ? (
                    <ChevronUp className="w-4 h-4 text-slate-400 shrink-0" />
                  ) : (
                    <ChevronDown className="w-4 h-4 text-slate-400 shrink-0" />
                  )}
                </button>

                {isOpen && (
                  <div className="p-4 pt-0 border-t border-slate-800/60 text-xs text-slate-300 leading-relaxed bg-slate-900/40">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-[11px] font-mono text-cyan-400 uppercase font-semibold">
                        Model Answer:
                      </span>
                      <button
                        onClick={() => handleCopy(idx, qa.answer)}
                        className="text-[11px] text-slate-400 hover:text-white flex items-center gap-1 transition-colors px-2 py-0.5 rounded bg-slate-800"
                      >
                        {copiedIndex === idx ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
                        <span>{copiedIndex === idx ? 'Copied' : 'Copy'}</span>
                      </button>
                    </div>
                    <p>{qa.answer}</p>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

    </div>
  );
};
