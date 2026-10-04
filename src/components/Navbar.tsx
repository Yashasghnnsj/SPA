import React from 'react';
import { ShieldCheck, Play, Pause, FileDown, BrainCircuit } from 'lucide-react';

export type ActiveTab = 'dashboard' | 'workflow' | 'ml-training' | 'code' | 'attack-lab' | 'viva';

interface NavbarProps {
  activeTab: ActiveTab;
  setActiveTab: (tab: ActiveTab) => void;
  isStreamRunning: boolean;
  onToggleStream: () => void;
  onOpenExportModal: () => void;
}

export const Navbar: React.FC<NavbarProps> = ({
  activeTab,
  setActiveTab,
  isStreamRunning,
  onToggleStream,
  onOpenExportModal,
}) => {
  return (
    <header className="sticky top-0 z-40 w-full border-b border-slate-800 bg-slate-950/90 backdrop-blur-md">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
        
        {/* Zone 1: Single text element wordmark */}
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center text-cyan-400">
            <ShieldCheck className="w-5 h-5" />
          </div>
          <button 
            onClick={() => setActiveTab('dashboard')} 
            className="text-lg font-bold tracking-tight text-white hover:text-cyan-400 transition-colors text-left"
          >
            FlinkGuard
          </button>
        </div>

        {/* Zone 2: Clean single-line text navigation links */}
        <nav className="hidden md:flex items-center gap-5 text-sm font-medium">
          <button
            onClick={() => setActiveTab('dashboard')}
            className={`whitespace-nowrap transition-colors ${
              activeTab === 'dashboard'
                ? 'text-cyan-400 font-semibold border-b-2 border-cyan-400 py-5'
                : 'text-slate-300 hover:text-white py-5'
            }`}
          >
            Real-Time NOC
          </button>

          <button
            onClick={() => setActiveTab('workflow')}
            className={`whitespace-nowrap transition-colors ${
              activeTab === 'workflow'
                ? 'text-cyan-400 font-semibold border-b-2 border-cyan-400 py-5'
                : 'text-slate-300 hover:text-white py-5'
            }`}
          >
            Project Workflow
          </button>

          <button
            onClick={() => setActiveTab('ml-training')}
            className={`whitespace-nowrap transition-colors flex items-center gap-1.5 ${
              activeTab === 'ml-training'
                ? 'text-purple-400 font-semibold border-b-2 border-purple-400 py-5'
                : 'text-slate-300 hover:text-white py-5'
            }`}
          >
            <BrainCircuit className="w-3.5 h-3.5 text-purple-400" />
            <span>TalkingData ML Training</span>
          </button>

          <button
            onClick={() => setActiveTab('code')}
            className={`whitespace-nowrap transition-colors ${
              activeTab === 'code'
                ? 'text-cyan-400 font-semibold border-b-2 border-cyan-400 py-5'
                : 'text-slate-300 hover:text-white py-5'
            }`}
          >
            Flink Code & Kafka
          </button>

          <button
            onClick={() => setActiveTab('attack-lab')}
            className={`whitespace-nowrap transition-colors ${
              activeTab === 'attack-lab'
                ? 'text-cyan-400 font-semibold border-b-2 border-cyan-400 py-5'
                : 'text-slate-300 hover:text-white py-5'
            }`}
          >
            Attack Lab
          </button>

          <button
            onClick={() => setActiveTab('viva')}
            className={`whitespace-nowrap transition-colors ${
              activeTab === 'viva'
                ? 'text-cyan-400 font-semibold border-b-2 border-cyan-400 py-5'
                : 'text-slate-300 hover:text-white py-5'
            }`}
          >
            Defense & Evaluation
          </button>
        </nav>

        {/* Zone 3: Primary action buttons */}
        <div className="flex items-center gap-3">
          <button
            onClick={onToggleStream}
            className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors flex items-center gap-1.5 border whitespace-nowrap ${
              isStreamRunning
                ? 'bg-amber-500/10 border-amber-500/30 text-amber-300 hover:bg-amber-500/20'
                : 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300 hover:bg-emerald-500/20'
            }`}
            title={isStreamRunning ? 'Pause live click ingestion' : 'Resume live click ingestion'}
          >
            {isStreamRunning ? <Pause className="w-3.5 h-3.5" /> : <Play className="w-3.5 h-3.5" />}
            <span>{isStreamRunning ? 'Pause' : 'Resume'}</span>
          </button>

          <button
            onClick={onOpenExportModal}
            className="px-3.5 py-1.5 text-xs font-semibold text-slate-950 bg-cyan-400 hover:bg-cyan-300 rounded-lg transition-colors flex items-center gap-1.5 shadow-sm whitespace-nowrap"
          >
            <FileDown className="w-3.5 h-3.5" />
            <span>Export Report</span>
          </button>
        </div>

      </div>
    </header>
  );
};
