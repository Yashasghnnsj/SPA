import React, { useState } from 'react';
import { ML_TRAINING_ARTIFACTS } from '../data/mlTrainingCode';
import { 
  BrainCircuit, 
  Copy, 
  Check, 
  Download, 
  FileCode, 
  Terminal, 
  Sparkles, 
  AlertCircle, 
  BarChart, 
  Database,
  ArrowRight
} from 'lucide-react';

export const MLTrainingView: React.FC = () => {
  const [selectedArtifactId, setSelectedArtifactId] = useState<string>(ML_TRAINING_ARTIFACTS[0].id);
  const [copied, setCopied] = useState(false);

  const selectedArtifact = ML_TRAINING_ARTIFACTS.find(a => a.id === selectedArtifactId) || ML_TRAINING_ARTIFACTS[0];

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
      <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl relative overflow-hidden">
        <div className="absolute right-0 top-0 w-96 h-full bg-gradient-to-l from-purple-500/10 to-transparent pointer-events-none" />
        <div className="max-w-3xl space-y-2">
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-purple-400">
            <BrainCircuit className="w-4 h-4" />
            <span>Kaggle TalkingData AdTracking Fraud Detection Dataset</span>
          </div>
          <h2 className="text-xl font-bold text-white">
            Local Machine Learning Model Training Pipeline
          </h2>
          <p className="text-xs text-slate-300 leading-relaxed">
            Train your XGBoost and Random Forest classifiers locally on the TalkingData dataset. 
            Includes the 10 stream behavioral feature engineering transformations, class imbalance handling, 
            PR-AUC evaluation, and PyFlink / Apache Flink real-time scoring integration.
          </p>
        </div>
      </div>

      {/* 4-Step Local Training Recipe Card */}
      <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-1.5">
            <Terminal className="w-4 h-4 text-cyan-400" />
            How to Train & Deploy Your Model Locally in 4 Steps:
          </h3>
          <span className="text-[11px] text-slate-500 font-mono">Python 3.10+ · Apache Flink 1.18</span>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-xs">
          <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1">
            <div className="text-cyan-400 font-bold font-mono">STEP 1. Install Dependencies</div>
            <code className="text-[11px] text-slate-300 block font-mono bg-slate-900 p-1.5 rounded mt-1">
              pip install -r requirements.txt
            </code>
            <p className="text-[11px] text-slate-400 mt-1">Installs xgboost, scikit-learn, pyflink, pandas, joblib.</p>
          </div>

          <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1">
            <div className="text-cyan-400 font-bold font-mono">STEP 2. Prepare Dataset</div>
            <code className="text-[11px] text-slate-300 block font-mono bg-slate-900 p-1.5 rounded mt-1">
              python generate_sample_talkingdata.py
            </code>
            <p className="text-[11px] text-slate-400 mt-1">Or place Kaggle's <code className="text-white font-mono">train_sample.csv</code> in folder.</p>
          </div>

          <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1">
            <div className="text-cyan-400 font-bold font-mono">STEP 3. Train Classifier</div>
            <code className="text-[11px] text-slate-300 block font-mono bg-slate-900 p-1.5 rounded mt-1">
              python train_talkingdata_model.py
            </code>
            <p className="text-[11px] text-slate-400 mt-1">Generates 10 features, trains XGBoost, saves <code className="text-white font-mono">fraud_model.pkl</code>.</p>
          </div>

          <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1">
            <div className="text-cyan-400 font-bold font-mono">STEP 4. Stream Inference</div>
            <code className="text-[11px] text-slate-300 block font-mono bg-slate-900 p-1.5 rounded mt-1">
              flink run -py flink_ml_scoring_udf.py
            </code>
            <p className="text-[11px] text-slate-400 mt-1">Flink scores incoming Kafka clicks within 2ms per event!</p>
          </div>
        </div>
      </div>

      {/* Code File Navigation & Download Controls */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2 overflow-x-auto pb-1">
          {ML_TRAINING_ARTIFACTS.map((artifact) => (
            <button
              key={artifact.id}
              onClick={() => {
                setSelectedArtifactId(artifact.id);
                setCopied(false);
              }}
              className={`px-3 py-1.5 text-xs font-mono rounded-lg transition-colors border whitespace-nowrap flex items-center gap-1.5 ${
                artifact.id === selectedArtifactId
                  ? 'bg-slate-800 text-purple-300 border-purple-500/50 shadow-sm font-semibold'
                  : 'bg-slate-900/60 text-slate-400 hover:text-white border-slate-800 hover:bg-slate-800/40'
              }`}
            >
              <FileCode className="w-3.5 h-3.5" />
              <span>{artifact.filename}</span>
            </button>
          ))}
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={handleCopy}
            className="px-3 py-1.5 text-xs font-medium text-slate-300 hover:text-white bg-slate-800 hover:bg-slate-700 border border-slate-700 rounded-lg transition-colors flex items-center gap-1.5"
          >
            {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
            <span>{copied ? 'Copied' : 'Copy Script'}</span>
          </button>

          <button
            onClick={handleDownload}
            className="px-3.5 py-1.5 text-xs font-semibold text-slate-950 bg-purple-400 hover:bg-purple-300 rounded-lg transition-colors flex items-center gap-1.5 shadow-sm"
          >
            <Download className="w-3.5 h-3.5" />
            <span>Download {selectedArtifact.filename}</span>
          </button>
        </div>
      </div>

      {/* Code Viewer */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
        <div className="px-6 py-3 border-b border-slate-800 bg-slate-950/60 flex items-center justify-between text-xs">
          <div>
            <div className="flex items-center gap-2 font-mono">
              <span className="text-white font-semibold">{selectedArtifact.filename}</span>
              <span className="text-slate-500">·</span>
              <span className="text-purple-400">{selectedArtifact.title}</span>
            </div>
            <p className="text-slate-400 text-[11px] mt-0.5">{selectedArtifact.description}</p>
          </div>
          <span className="text-[11px] text-slate-500 font-mono">
            {selectedArtifact.code.split('\n').length} lines
          </span>
        </div>

        <pre className="p-6 bg-slate-950 text-xs font-mono text-slate-200 overflow-x-auto leading-relaxed max-h-[600px] overflow-y-auto">
          <code>{selectedArtifact.code}</code>
        </pre>
      </div>

      {/* Evaluation & Imbalance Note */}
      <div className="p-4 bg-slate-900/60 border border-slate-800 rounded-xl space-y-2 text-xs text-slate-400">
        <div className="flex items-center gap-2 text-slate-200 font-semibold">
          <AlertCircle className="w-4 h-4 text-amber-400" />
          <span>Why PR-AUC and Recall Matter More than Accuracy for TalkingData:</span>
        </div>
        <p className="leading-relaxed">
          The TalkingData dataset is heavily imbalanced: legitimate conversions are only ~0.2% (<code className="text-white font-mono">is_attributed=1</code>), 
          while non-converting/fraudulent clicks make up ~99.8%. A naive classifier predicting "Fraud" on everything would achieve 
          99.8% raw accuracy while completely failing to identify true conversions! That is why our training code uses 
          <code className="text-white font-mono">scale_pos_weight</code>, evaluates <strong className="text-slate-200">Precision-Recall AUC (PR-AUC)</strong>, 
          and optimizes the decision threshold at <strong className="text-slate-200">0.60</strong>.
        </p>
      </div>

    </div>
  );
};
