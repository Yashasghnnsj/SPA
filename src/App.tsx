import React, { useState, useEffect } from 'react';
import { Navbar, ActiveTab } from './components/Navbar';
import { DashboardView } from './components/DashboardView';
import { WorkflowView } from './components/WorkflowView';
import { MLTrainingView } from './components/MLTrainingView';
import { CodeArtifactsView } from './components/CodeArtifactsView';
import { AttackLabView } from './components/AttackLabView';
import { VivaDefenseView } from './components/VivaDefenseView';
import { ClickDetailModal } from './components/ClickDetailModal';
import { ExportReportModal } from './components/ExportReportModal';
import { AdClickEvent, FlinkEngineState, KafkaConsumerLagState } from './types/fraud';
import { globalFlinkStream } from './services/streamSimulator';

export default function App() {
  const [activeTab, setActiveTab] = useState<ActiveTab>('dashboard');
  const [isStreamRunning, setIsStreamRunning] = useState<boolean>(true);
  const [engineState, setEngineState] = useState<FlinkEngineState>(globalFlinkStream.getState());
  const [kafkaState, setKafkaState] = useState<KafkaConsumerLagState>(globalFlinkStream.getKafkaLagState());
  const [events, setEvents] = useState<AdClickEvent[]>([]);
  const [selectedClick, setSelectedClick] = useState<AdClickEvent | null>(null);
  const [isExportModalOpen, setIsExportModalOpen] = useState<boolean>(false);

  useEffect(() => {
    globalFlinkStream.start();

    const unsubscribe = globalFlinkStream.subscribe((newEvent, newEngineState, newKafkaState) => {
      setEvents((prev) => [newEvent, ...prev.slice(0, 79)]);
      setEngineState(newEngineState);
      if (newKafkaState) {
        setKafkaState({ ...newKafkaState });
      }
    });

    return () => {
      unsubscribe();
      globalFlinkStream.stop();
    };
  }, []);

  const handleToggleStream = () => {
    if (isStreamRunning) {
      globalFlinkStream.stop();
      setIsStreamRunning(false);
    } else {
      globalFlinkStream.start();
      setIsStreamRunning(true);
    }
  };

  const handleUpdateEngineState = (newConfig: Partial<FlinkEngineState>) => {
    globalFlinkStream.updateConfig(newConfig);
    setEngineState(globalFlinkStream.getState());
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      
      {/* Strict 3-Zone Top Navigation Bar */}
      <Navbar
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        isStreamRunning={isStreamRunning}
        onToggleStream={handleToggleStream}
        onOpenExportModal={() => setIsExportModalOpen(true)}
      />

      {/* Main Container */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {activeTab === 'dashboard' && (
          <DashboardView
            engineState={engineState}
            kafkaState={kafkaState}
            events={events}
            isStreamRunning={isStreamRunning}
            onSelectClick={(click) => setSelectedClick(click)}
            onUpdateEngineState={handleUpdateEngineState}
          />
        )}

        {activeTab === 'workflow' && (
          <WorkflowView />
        )}

        {activeTab === 'ml-training' && (
          <MLTrainingView />
        )}

        {activeTab === 'code' && (
          <CodeArtifactsView />
        )}

        {activeTab === 'attack-lab' && (
          <AttackLabView 
            onAttackLaunched={() => {
              // Stay on current tab or switch to dashboard
            }}
          />
        )}

        {activeTab === 'viva' && (
          <VivaDefenseView />
        )}
      </main>

      {/* Forensic Inspection Modal */}
      {selectedClick && (
        <ClickDetailModal
          click={selectedClick}
          onClose={() => setSelectedClick(null)}
        />
      )}

      {/* Export Report Modal */}
      {isExportModalOpen && (
        <ExportReportModal
          engineState={engineState}
          onClose={() => setIsExportModalOpen(false)}
        />
      )}

      {/* Quiet Footer */}
      <footer className="border-t border-slate-900 bg-slate-950/80 py-6 text-xs text-slate-500">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col md:flex-row items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-400">FlinkGuard</span>
            <span>·</span>
            <span>Real-Time Advertisement Click Fraud Detection System</span>
            <span>·</span>
            <span>Kaggle TalkingData ML + Apache Flink & Kafka</span>
          </div>
          <div className="flex items-center gap-4 text-slate-500 font-mono">
            <span>Apache Flink 1.18</span>
            <span>Apache Kafka (KRaft)</span>
            <span>XGBoost / RandomForest</span>
            <span>PostgreSQL & Parquet</span>
          </div>
        </div>
      </footer>

    </div>
  );
}
