import React, { useState } from 'react';
import { WORKFLOW_STAGES, WorkflowStage } from '../data/projectWorkflowData';
import { 
  ChevronRight, 
  ArrowRight, 
  Layers, 
  Database, 
  Zap, 
  Cpu, 
  HardDrive, 
  Monitor, 
  CheckCircle2, 
  Copy, 
  Check, 
  Terminal,
  FileCode2,
  Workflow
} from 'lucide-react';

export const WorkflowView: React.FC = () => {
  const [activeStageId, setActiveStageId] = useState<string>(WORKFLOW_STAGES[0].id);
  const [copiedCodeTitle, setCopiedCodeTitle] = useState<string | null>(null);

  const activeStage = WORKFLOW_STAGES.find(s => s.id === activeStageId) || WORKFLOW_STAGES[0];

  const handleCopyCode = (title: string, code: string) => {
    navigator.clipboard.writeText(code);
    setCopiedCodeTitle(title);
    setTimeout(() => setCopiedCodeTitle(null), 2000);
  };

  return (
    <div className="space-y-8">
      
      {/* Header Banner */}
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl relative overflow-hidden">
        <div className="absolute right-0 top-0 w-96 h-full bg-gradient-to-l from-cyan-500/10 to-transparent pointer-events-none" />
        <div className="max-w-3xl space-y-2">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-cyan-400">
            <Workflow className="w-4 h-4" />
            <span>Complete Project Workflow & Technical Blueprint</span>
          </div>
          <h2 className="text-2xl font-bold tracking-tight text-white">
            Distributed Stream Processing Architecture
          </h2>
          <p className="text-sm text-slate-300 leading-relaxed">
            A comprehensive, end-to-end guide detailing ingestion topologies, event-time watermarking, 
            Apache Flink stateful sliding windows, Complex Event Processing (CEP), RocksDB state management, 
            and real-time OLAP visualization.
          </p>
        </div>
      </div>

      {/* Interactive Architecture Flow Diagram */}
      <div className="p-6 bg-slate-900/80 border border-slate-800 rounded-xl space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-white uppercase tracking-wider">
            End-to-End System Pipeline Topology
          </h3>
          <span className="text-xs text-slate-400">Click any stage to inspect detailed specifications</span>
        </div>

        {/* Pipeline Nodes Flow */}
        <div className="grid grid-cols-2 md:grid-cols-6 gap-3 pt-2">
          {WORKFLOW_STAGES.map((stage, idx) => {
            const isSelected = stage.id === activeStageId;
            return (
              <button
                key={stage.id}
                onClick={() => setActiveStageId(stage.id)}
                className={`p-3 text-left rounded-lg transition-all border relative flex flex-col justify-between ${
                  isSelected
                    ? 'bg-cyan-950/40 border-cyan-400 shadow-md shadow-cyan-950/50'
                    : 'bg-slate-950/60 border-slate-800 hover:border-slate-700 hover:bg-slate-900/60'
                }`}
              >
                <div>
                  <div className="flex items-center justify-between text-[11px] font-mono text-slate-500 mb-1">
                    <span>STAGE {stage.stepNumber}</span>
                    <span className={`w-2 h-2 rounded-full ${isSelected ? 'bg-cyan-400' : 'bg-slate-700'}`} />
                  </div>
                  <h4 className={`text-xs font-semibold line-clamp-2 ${isSelected ? 'text-cyan-300' : 'text-slate-200'}`}>
                    {stage.title.split('&')[0]}
                  </h4>
                </div>
                <div className="text-[10px] text-slate-500 mt-3 font-mono">
                  {stage.category}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Active Stage Detailed Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

        {/* Left Column: Stage Navigation & Key Highlights */}
        <div className="space-y-4">
          <div className="p-5 bg-slate-900 border border-slate-800 rounded-xl space-y-4">
            <div>
              <span className="text-xs font-mono text-cyan-400 uppercase font-semibold">
                Stage {activeStage.stepNumber} of 06
              </span>
              <h3 className="text-lg font-bold text-white mt-1">
                {activeStage.title}
              </h3>
              <p className="text-xs text-slate-400 mt-1">
                {activeStage.subtitle}
              </p>
            </div>

            <div className="pt-2 border-t border-slate-800 space-y-2">
              <span className="text-xs font-semibold text-slate-300 uppercase tracking-wider">
                Key Components Covered:
              </span>
              <ul className="text-xs text-slate-400 space-y-2">
                {activeStage.keyComponents.map((comp, idx) => (
                  <li key={idx} className="flex items-start gap-2">
                    <CheckCircle2 className="w-3.5 h-3.5 text-cyan-400 shrink-0 mt-0.5" />
                    <span>{comp}</span>
                  </li>
                ))}
              </ul>
            </div>

            <div className="pt-2 border-t border-slate-800">
              <span className="text-xs font-semibold text-slate-300 uppercase tracking-wider block mb-2">
                Technologies Employed:
              </span>
              <div className="flex flex-wrap gap-1.5">
                {activeStage.techStack.map((tech) => (
                  <span key={tech} className="px-2 py-0.5 text-[11px] font-mono bg-slate-800 border border-slate-700 rounded text-slate-300">
                    {tech}
                  </span>
                ))}
              </div>
            </div>
          </div>

          {/* Quick Stage Switcher List */}
          <div className="p-3 bg-slate-900 border border-slate-800 rounded-xl space-y-1">
            <span className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider px-2 py-1 block">
              Jump to Stage:
            </span>
            {WORKFLOW_STAGES.map((s) => (
              <button
                key={s.id}
                onClick={() => setActiveStageId(s.id)}
                className={`w-full text-left px-3 py-2 rounded-lg text-xs transition-colors flex items-center justify-between ${
                  s.id === activeStageId
                    ? 'bg-slate-800 text-white font-medium'
                    : 'text-slate-400 hover:text-white hover:bg-slate-800/40'
                }`}
              >
                <span>{s.stepNumber}. {s.title}</span>
                <ChevronRight className="w-3.5 h-3.5 text-slate-600" />
              </button>
            ))}
          </div>
        </div>

        {/* Right 2 Columns: Deep Technical Content, Code & Diagrams */}
        <div className="lg:col-span-2 space-y-6">
          <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl space-y-6">
            
            {/* Stage Summary */}
            <div className="prose prose-invert max-w-none">
              <h4 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
                Stage Executive Brief
              </h4>
              <p className="text-sm text-slate-200 leading-relaxed mt-2">
                {activeStage.summary}
              </p>
            </div>

            {/* Stage Technical Detail Blocks */}
            <div className="space-y-6 pt-4 border-t border-slate-800">
              {activeStage.technicalDetails.map((detail, idx) => (
                <div key={idx} className="space-y-3">
                  <h4 className="text-base font-semibold text-white flex items-center gap-2">
                    <span className="w-1.5 h-1.5 rounded-full bg-cyan-400" />
                    {detail.heading}
                  </h4>
                  <p className="text-xs text-slate-300 leading-relaxed whitespace-pre-line">
                    {detail.content}
                  </p>

                  {/* Flow Diagram if present */}
                  {detail.diagramFlow && (
                    <div className="p-4 bg-slate-950 border border-slate-800 rounded-lg space-y-2">
                      <span className="text-[11px] font-semibold text-slate-400 uppercase tracking-wider block">
                        Pipeline Data Transit Sequence:
                      </span>
                      <div className="flex flex-col md:flex-row items-center gap-2 overflow-x-auto py-2">
                        {detail.diagramFlow.map((step, sIdx) => (
                          <React.Fragment key={sIdx}>
                            <div className="px-3 py-2 bg-slate-900 border border-slate-800 rounded text-xs font-mono text-cyan-300 whitespace-nowrap text-center">
                              {step}
                            </div>
                            {sIdx < detail.diagramFlow!.length - 1 && (
                              <ArrowRight className="w-4 h-4 text-slate-600 shrink-0 hidden md:block" />
                            )}
                          </React.Fragment>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Code Snippet if present */}
                  {detail.codeSnippet && (
                    <div className="space-y-2 pt-2">
                      <div className="flex items-center justify-between px-3 py-1.5 bg-slate-950 border border-slate-800 rounded-t-lg">
                        <div className="flex items-center gap-2 text-xs font-mono text-slate-400">
                          <FileCode2 className="w-3.5 h-3.5 text-cyan-400" />
                          <span>{detail.codeSnippet.title}</span>
                        </div>
                        <button
                          onClick={() => handleCopyCode(detail.codeSnippet!.title, detail.codeSnippet!.code)}
                          className="text-xs text-slate-400 hover:text-white flex items-center gap-1 transition-colors px-2 py-0.5 rounded hover:bg-slate-800"
                        >
                          {copiedCodeTitle === detail.codeSnippet.title ? (
                            <Check className="w-3.5 h-3.5 text-emerald-400" />
                          ) : (
                            <Copy className="w-3.5 h-3.5" />
                          )}
                          <span>{copiedCodeTitle === detail.codeSnippet.title ? 'Copied' : 'Copy'}</span>
                        </button>
                      </div>
                      <pre className="p-4 bg-slate-950 border-x border-b border-slate-800 rounded-b-lg text-xs font-mono text-slate-200 overflow-x-auto max-h-96">
                        {detail.codeSnippet.code}
                      </pre>
                    </div>
                  )}
                </div>
              ))}
            </div>

          </div>
        </div>

      </div>

    </div>
  );
};
