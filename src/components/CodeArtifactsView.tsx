import React, { useState } from 'react';
import { CODE_ARTIFACTS, CodeArtifact } from '../data/codeArtifacts';
import { Copy, Check, Download, FileCode, Terminal, Sparkles } from 'lucide-react';

export const CodeArtifactsView: React.FC = () => {
  const [selectedArtifactId, setSelectedArtifactId] = useState<string>(CODE_ARTIFACTS[0].id);
  const [copied, setCopied] = useState(false);

  const selectedArtifact = CODE_ARTIFACTS.find(a => a.id === selectedArtifactId) || CODE_ARTIFACTS[0];

  const handleCopy = () => {
    navigator.clipboard.writeText(selectedArtifact.code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const blob = new Blob([selectedArtifact.code], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = selectedArtifact.filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      
      {/* Header Banner */}
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl flex flex-wrap items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-cyan-400 mb-1">
            <FileCode className="w-4 h-4" />
            <span>Complete Project Source Code & Infrastructure Repository</span>
          </div>
          <h2 className="text-xl font-bold text-white">
            Apache Flink Production Code Artifacts
          </h2>
          <p className="text-xs text-slate-400 mt-1">
            Production-grade implementations in Java 17 and Python 3.10 with Docker Compose orchestration.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={handleCopy}
            className="px-3 py-1.5 text-xs font-medium text-slate-300 hover:text-white bg-slate-800 hover:bg-slate-700 border border-slate-700 rounded-lg transition-colors flex items-center gap-1.5"
          >
            {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
            <span>{copied ? 'Copied' : 'Copy Code'}</span>
          </button>

          <button
            onClick={handleDownload}
            className="px-3.5 py-1.5 text-xs font-semibold text-slate-950 bg-cyan-400 hover:bg-cyan-300 rounded-lg transition-colors flex items-center gap-1.5 shadow-sm"
          >
            <Download className="w-3.5 h-3.5" />
            <span>Download {selectedArtifact.filename}</span>
          </button>
        </div>
      </div>

      {/* Code File Navigation Tabs */}
      <div className="flex items-center gap-2 overflow-x-auto pb-1">
        {CODE_ARTIFACTS.map((artifact) => (
          <button
            key={artifact.id}
            onClick={() => {
              setSelectedArtifactId(artifact.id);
              setCopied(false);
            }}
            className={`px-4 py-2 text-xs font-mono rounded-lg transition-colors border whitespace-nowrap flex items-center gap-2 ${
              artifact.id === selectedArtifactId
                ? 'bg-slate-800 text-cyan-300 border-cyan-500/50 shadow-sm font-semibold'
                : 'bg-slate-900/60 text-slate-400 hover:text-white border-slate-800 hover:bg-slate-800/40'
            }`}
          >
            <FileCode className="w-3.5 h-3.5" />
            <span>{artifact.filename}</span>
            <span className="text-[10px] text-slate-500">({artifact.category})</span>
          </button>
        ))}
      </div>

      {/* Code Viewer Panel */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
        
        {/* File Meta Header */}
        <div className="px-6 py-3 border-b border-slate-800 bg-slate-950/60 flex items-center justify-between text-xs">
          <div className="space-y-0.5">
            <div className="flex items-center gap-2 font-mono">
              <span className="text-white font-semibold">{selectedArtifact.filename}</span>
              <span className="text-slate-500">·</span>
              <span className="text-cyan-400">{selectedArtifact.language.toUpperCase()}</span>
            </div>
            <p className="text-slate-400 text-[11px]">{selectedArtifact.description}</p>
          </div>

          <div className="flex items-center gap-2">
            <span className="text-[11px] text-slate-500 font-mono">
              {selectedArtifact.code.split('\n').length} lines
            </span>
          </div>
        </div>

        {/* Code Content */}
        <div className="relative">
          <pre className="p-6 bg-slate-950 text-xs font-mono text-slate-200 overflow-x-auto leading-relaxed max-h-[640px] overflow-y-auto">
            <code>{selectedArtifact.code}</code>
          </pre>
        </div>

        {/* File Execution Note */}
        <div className="px-6 py-3 border-t border-slate-800 bg-slate-950/80 flex items-center gap-2 text-xs text-slate-400">
          <Terminal className="w-3.5 h-3.5 text-cyan-400" />
          <span>
            To run this in your local test environment: Start infrastructure via{' '}
            <code className="px-1.5 py-0.5 bg-slate-800 rounded text-cyan-300 font-mono">docker-compose up -d</code>{' '}
            and submit the compiled JAR using{' '}
            <code className="px-1.5 py-0.5 bg-slate-800 rounded text-cyan-300 font-mono">flink run -c com.fraudguard.stream.ClickFraudDetectionJob target/fraud-job.jar</code>
          </span>
        </div>

      </div>

    </div>
  );
};
