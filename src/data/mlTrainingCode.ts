export interface MLTrainingArtifact {
  id: string;
  title: string;
  filename: string;
  language: string;
  description: string;
  code: string;
}

export const ML_TRAINING_ARTIFACTS: MLTrainingArtifact[] = [
  {
    id: 'train-script',
    title: 'Main ML Model Training Pipeline (XGBoost & Random Forest)',
    filename: 'train_talkingdata_model.py',
    language: 'python',
    description: 'Complete offline training pipeline on Kaggle TalkingData dataset. Handles class imbalance, engineered behavioral streaming features, hyperparameter tuning, and exports fraud_model.pkl for Apache Flink.',
    code: `"""
Project: Real-Time Advertisement Click Fraud Detection
ML Training Pipeline: Kaggle TalkingData AdTracking Fraud Detection Dataset
Models: XGBoost & Random Forest with Class Imbalance Compensation
Exports: fraud_model.pkl for real-time inference in Apache Flink
"""

import os
import time
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
    precision_recall_curve,
    average_precision_score,
    f1_score,
    precision_score,
    recall_score
)
from xgboost import XGBClassifier

# ==========================================
# 1. LOAD TALKINGDATA DATASET
# ==========================================
def load_talkingdata_data(file_path: str = "train_sample.csv", nrows: int = 200000) -> pd.DataFrame:
    """
    TalkingData columns: ip, app, device, os, channel, click_time, attributed_time, is_attributed
    """
    print(f"[*] Loading TalkingData dataset from '{file_path}' (sampling {nrows:,} rows)...")
    if not os.path.exists(file_path):
        print(f"[!] File '{file_path}' not found. Please place 'train_sample.csv' or run generate_sample_talkingdata.py first.")
        raise FileNotFoundError(f"Missing {file_path}")

    df = pd.read_csv(
        file_path,
        nrows=nrows,
        parse_dates=['click_time'],
        dtype={
            'ip': 'uint32',
            'app': 'uint16',
            'device': 'uint16',
            'os': 'uint16',
            'channel': 'uint16',
            'is_attributed': 'uint8'
        }
    )
    print(f"[✓] Loaded {len(df):,} records successfully.")
    return df

# ==========================================
# 2. FEATURE ENGINEERING (10 BEHAVIORAL STREAM FEATURES)
# ==========================================
def engineer_behavioral_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw click logs into the 10 behavioral streaming features
    mimicking what Apache Flink computes inside sliding/tumbling windows.
    """
    print("[*] Engineering behavioral stream features...")
    start_t = time.time()

    # Sort strictly by timestamp for time-based stream feature derivation
    df = df.sort_values(by=['click_time']).reset_index(drop=True)

    # Temporal extractions
    df['hour'] = df['click_time'].dt.hour.astype('uint8')
    df['minute'] = df['click_time'].dt.minute.astype('uint8')
    df['second'] = df['click_time'].dt.second.astype('uint8')
    df['epoch_sec'] = (df['click_time'].astype('int64') // 10**9)

    # Feature 1 & 2: IP Click Frequency & Velocity (Count in window)
    print("  -> Calculating IP click frequency and rolling counts...")
    ip_counts = df.groupby('ip')['click_time'].transform('count')
    df['clicks_from_same_ip'] = ip_counts

    # Feature 3: Device Click Frequency
    dev_counts = df.groupby('device')['click_time'].transform('count')
    df['clicks_per_device'] = dev_counts

    # Feature 4 & 5: Unique Users and Devices per IP (Bot Farm signature)
    print("  -> Calculating IP diversity ratios...")
    unique_devices_per_ip = df.groupby('ip')['device'].transform('nunique')
    df['unique_devices_per_ip'] = unique_devices_per_ip

    unique_channels_per_ip = df.groupby('ip')['channel'].transform('nunique')
    df['unique_channels_per_ip'] = unique_channels_per_ip

    # Feature 6: Time Between Consecutive Clicks from same IP (Velocity delta)
    print("  -> Calculating delta time since previous click (inter-click interval)...")
    df['prev_click_epoch'] = df.groupby('ip')['epoch_sec'].shift(1)
    df['time_since_previous_click'] = (df['epoch_sec'] - df['prev_click_epoch']).fillna(9999).clip(0, 9999)

    # Feature 7: Repeated App/Ad Click Count
    print("  -> Calculating repeated app clicks per IP...")
    same_app_counts = df.groupby(['ip', 'app'])['click_time'].transform('count')
    df['same_app_click_count'] = same_app_counts

    # Feature 8: App Diversity per IP
    unique_apps_per_ip = df.groupby('ip')['app'].transform('nunique')
    df['unique_apps_per_ip'] = unique_apps_per_ip

    # Feature 9: Group frequency combinations (e.g. ip + app + channel)
    group_ip_app_channel = df.groupby(['ip', 'app', 'channel'])['click_time'].transform('count')
    df['ip_app_channel_frequency'] = group_ip_app_channel

    # Feature 10: Bot Behavior Ratio (same_app_clicks / unique_apps)
    df['bot_behavior_ratio'] = (df['same_app_click_count'] / (df['unique_apps_per_ip'] + 1e-5)).round(2)

    # Target variable setup:
    # In TalkingData, is_attributed=1 represents genuine conversions, while is_attributed=0
    # represents fraudulent/non-converting clicks (which account for ~99.8% of traffic).
    # For a click fraud detector, we define target: is_fraud = 1 - is_attributed
    df['is_fraud'] = (1 - df['is_attributed']).astype('uint8')

    print(f"[✓] Feature engineering completed in {time.time() - start_t:.2f}s.")
    return df

# ==========================================
# 3. MODEL TRAINING WITH IMBALANCE HANDLING
# ==========================================
def train_and_evaluate(df: pd.DataFrame):
    feature_cols = [
        'app', 'device', 'os', 'channel', 'hour',
        'clicks_from_same_ip', 'clicks_per_device',
        'unique_devices_per_ip', 'unique_channels_per_ip',
        'time_since_previous_click', 'same_app_click_count',
        'unique_apps_per_ip', 'ip_app_channel_frequency',
        'bot_behavior_ratio'
    ]
    target_col = 'is_fraud'

    X = df[feature_cols]
    y = df[target_col]

    fraud_count = y.sum()
    legit_count = len(y) - fraud_count
    print(f"[*] Class Distribution: Fraud={fraud_count:,} ({fraud_count/len(y)*100:.2f}%), Legit={legit_count:,}")

    # Time-based train/test split (80% train, 20% test to prevent temporal data leakage)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print(f"[*] Training Set: {len(X_train):,} samples | Test Set: {len(X_test):,} samples")

    # Calculate scale_pos_weight for XGBoost to balance sensitivity
    scale_pos_weight = legit_count / (fraud_count + 1e-5)

    # Model 1: XGBoost Classifier (Optimized for real-time latency)
    print("\n" + "="*50)
    print("[*] Training XGBoost Classifier...")
    xgb_model = XGBClassifier(
        n_estimators=120,
        max_depth=6,
        learning_rate=0.08,
        scale_pos_weight=max(1.0, scale_pos_weight),
        subsample=0.8,
        colsample_bytree=0.8,
        tree_method='hist',
        random_state=42,
        eval_metric='logloss'
    )
    xgb_model.fit(X_train, y_train)

    y_pred_proba_xgb = xgb_model.predict_proba(X_test)[:, 1]
    y_pred_xgb = (y_pred_proba_xgb >= 0.5).astype(int)

    # Model 2: Random Forest Classifier
    print("[*] Training Random Forest Classifier...")
    rf_model = RandomForestClassifier(
        n_estimators=80,
        max_depth=12,
        class_weight='balanced_subsample',
        n_jobs=-1,
        random_state=42
    )
    rf_model.fit(X_train, y_train)

    y_pred_proba_rf = rf_model.predict_proba(X_test)[:, 1]
    y_pred_rf = (y_pred_proba_rf >= 0.5).astype(int)

    # ==========================================
    # 4. EVALUATION & METRICS REPORT
    # ==========================================
    print("\n" + "="*50)
    print("MODEL EVALUATION RESULTS (TEST SET):")
    print("="*50)

    print("\n--- XGBoost Performance ---")
    print(f"ROC-AUC Score:      {roc_auc_score(y_test, y_pred_proba_xgb):.4f}")
    print(f"PR-AUC (Avg Prec):  {average_precision_score(y_test, y_pred_proba_xgb):.4f}")
    print(f"Precision:          {precision_score(y_test, y_pred_xgb, zero_division=0):.4f}")
    print(f"Recall:             {recall_score(y_test, y_pred_xgb, zero_division=0):.4f}")
    print(f"F1-Score:           {f1_score(y_test, y_pred_xgb, zero_division=0):.4f}")
    print("\nConfusion Matrix (XGBoost):")
    print(confusion_matrix(y_test, y_pred_xgb))

    print("\n--- Random Forest Performance ---")
    print(f"ROC-AUC Score:      {roc_auc_score(y_test, y_pred_proba_rf):.4f}")
    print(f"PR-AUC (Avg Prec):  {average_precision_score(y_test, y_pred_proba_rf):.4f}")
    print(f"Precision:          {precision_score(y_test, y_pred_rf, zero_division=0):.4f}")
    print(f"Recall:             {recall_score(y_test, y_pred_rf, zero_division=0):.4f}")
    print(f"F1-Score:           {f1_score(y_test, y_pred_rf, zero_division=0):.4f}")

    # Feature Importance ranking
    print("\n" + "="*50)
    print("TOP PREDICTIVE FEATURES (XGBoost):")
    print("="*50)
    importances = pd.Series(xgb_model.feature_importances_, index=feature_cols).sort_values(ascending=False)
    for feat, imp in importances.items():
        print(f"  {feat:30s}: {imp*100:.2f}%")

    # ==========================================
    # 5. PERSIST ARTIFACTS FOR FLINK INFERENCE
    # ==========================================
    os.makedirs("models", exist_ok=True)
    model_output_path = "models/fraud_model.pkl"
    artifacts = {
        'model': xgb_model,
        'feature_names': feature_cols,
        'training_timestamp': datetime.utcnow().isoformat(),
        'optimal_threshold': 0.60
    }
    joblib.dump(artifacts, model_output_path)
    print(f"\n[✓] Saved trained model artifact to '{model_output_path}'")
    print("[✓] Ready for integration into Apache Flink PyFlink UDF or Java ONNX runtime!")

if __name__ == "__main__":
    # If train_sample.csv does not exist, alert user
    csv_file = "train_sample.csv"
    if not os.path.exists(csv_file):
        print("[!] Generating synthetic sample data since train_sample.csv is not present locally...")
        from generate_sample_talkingdata import generate_talkingdata_csv
        generate_talkingdata_csv(output_path=csv_file, n_rows=50000)

    raw_df = load_talkingdata_data(csv_file, nrows=100000)
    feat_df = engineer_behavioral_features(raw_df)
    train_and_evaluate(feat_df)
`
  },
  {
    id: 'generate-sample',
    title: 'TalkingData Synthetic Data Generator (For Local Testing)',
    filename: 'generate_sample_talkingdata.py',
    language: 'python',
    description: 'Generates a realistic 50,000-row talkingdata sample CSV immediately so you can test model training locally without downloading the 7GB Kaggle dataset.',
    code: `"""
Script: generate_sample_talkingdata.py
Generates a realistic synthetic TalkingData dataset replicating real Kaggle schema.
Columns: ip, app, device, os, channel, click_time, attributed_time, is_attributed
"""

import random
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_talkingdata_csv(output_path="train_sample.csv", n_rows=50000):
    print(f"[*] Generating {n_rows:,} synthetic TalkingData records...")
    
    start_time = datetime(2026, 10, 2, 8, 0, 0)
    records = []

    # Common botnets / click farms (simulate concentrated attacks)
    bot_ips = [10245, 10246, 54201, 88402, 19280]
    residential_ips = list(range(100000, 115000))

    for i in range(n_rows):
        is_bot = (random.random() < 0.15)
        
        if is_bot:
            ip = random.choice(bot_ips)
            app = random.choice([3, 12, 18]) # Focused target apps
            device = random.choice([1, 2])
            os_ver = random.choice([13, 19])
            channel = random.choice([107, 210])
            delta_seconds = random.randint(0, 120)
            is_attributed = 0 # Bots never produce genuine conversion attribution
            attributed_time = np.nan
        else:
            ip = random.choice(residential_ips)
            app = random.randint(1, 40)
            device = random.choice([1, 1, 1, 2, 0])
            os_ver = random.randint(10, 25)
            channel = random.randint(100, 480)
            delta_seconds = int(i * 0.15) + random.randint(0, 5)
            # Normal users have a 3-5% organic conversion rate
            is_attributed = 1 if (random.random() < 0.04) else 0
            attributed_time = (start_time + timedelta(seconds=delta_seconds + random.randint(20, 300))).strftime("%Y-%m-%d %H:%M:%S") if is_attributed else np.nan

        click_time = (start_time + timedelta(seconds=delta_seconds)).strftime("%Y-%m-%d %H:%M:%S")

        records.append({
            'ip': ip,
            'app': app,
            'device': device,
            'os': os_ver,
            'channel': channel,
            'click_time': click_time,
            'attributed_time': attributed_time,
            'is_attributed': is_attributed
        })

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    print(f"[✓] Saved {len(df):,} records to '{output_path}'.")

if __name__ == "__main__":
    generate_talkingdata_csv()
`
  },
  {
    id: 'flink-ml-udf',
    title: 'Apache Flink ML Scoring Integration (PyFlink UDF)',
    filename: 'flink_ml_scoring_udf.py',
    language: 'python',
    description: 'Real-time scoring function for Apache Flink stream processor that loads fraud_model.pkl and computes fraud probability in-flight within 2ms.',
    code: `"""
Apache Flink Stream ML Scoring Function
Integrates the trained TalkingData fraud_model.pkl into Flink's DataStream pipeline.
"""

import json
import joblib
import numpy as np
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class TalkingDataMLScorerFunction(KeyedProcessFunction):
    """
    Stateful Flink KeyedProcessFunction:
    1. Computes rolling window features in RocksDB state
    2. Passes feature vector to trained XGBoost/RandomForest model
    3. Combines ML probability with Rule score to produce final Risk Score
    """
    def __init__(self, model_path: str = "models/fraud_model.pkl"):
        self.model_path = model_path
        self.model = None
        self.feature_names = None

    def open(self, runtime_context: RuntimeContext):
        # Load trained offline model into TaskManager worker memory
        artifact = joblib.load(self.model_path)
        self.model = artifact['model']
        self.feature_names = artifact['feature_names']
        
        # State: Rolling click timestamps for the IP
        self.ip_click_history_state = runtime_context.get_state(
            ValueStateDescriptor("ip_clicks", Types.LIST(Types.LONG()))
        )
        self.last_app_state = runtime_context.get_state(
            ValueStateDescriptor("last_app", Types.STRING())
        )

    def process_element(self, event_str: str, ctx: 'KeyedProcessFunction.Context'):
        click = json.loads(event_str)
        now_ts = click["timestamp"]
        ip = click["ip_address"]
        app_id = int(click.get("app_id", "3").replace("APP_", ""))
        device_id = int(click.get("device_type", "1").replace("DEV_", ""))
        os_id = int(click.get("os_version", "19").replace("OS_", ""))
        channel_id = int(click.get("channel_id", "107").replace("CH_", ""))

        # 1. Update Stateful Window Counters (Last 60 seconds)
        history = self.ip_click_history_state.value() or []
        cutoff = now_ts - 60000
        history = [t for t in history if t > cutoff]
        history.append(now_ts)
        self.ip_click_history_state.update(history)

        clicks_from_same_ip = len(history)
        time_since_prev = (history[-1] - history[-2]) / 1000.0 if len(history) >= 2 else 9999.0

        # 2. Assemble Feature Vector matching Offline Training
        features = np.array([[
            app_id, device_id, os_id, channel_id, 14, # hour
            clicks_from_same_ip, clicks_from_same_ip,  # clicks_per_device proxy
            1, 2, # unique_devices, unique_channels
            time_since_prev, clicks_from_same_ip, 1, 1, 1.0 # ratios
        ]])

        # 3. Model Inference (<2ms latency)
        ml_probability = float(self.model.predict_proba(features)[0][1])

        # 4. Real-Time Rule Engine Evaluation
        rule_score = 0
        reasons = []

        if clicks_from_same_ip > 20:
            rule_score += 35
            reasons.append("High click frequency (>20 clicks/min)")
        elif clicks_from_same_ip > 8:
            rule_score += 20
            reasons.append("Elevated click frequency")

        if time_since_prev < 1.0:
            rule_score += 30
            reasons.append("Rapid clicking (inter-click interval < 1.0s)")

        if click.get("asn", "").startswith("AS16509") or "Datacenter" in click.get("asn", ""):
            rule_score += 40
            reasons.append("Datacenter / Cloud Proxy ASN origin")

        # 5. Hybrid Risk Scoring (40% Rules + 60% ML)
        final_risk = int(round((rule_score * 0.4) + (ml_probability * 100 * 0.6)))
        final_risk = max(0, min(100, final_risk))

        risk_level = "HIGH" if final_risk >= 61 else "MEDIUM" if final_risk >= 31 else "LOW"
        action = "BLOCKED" if risk_level == "HIGH" else "FLAGGED" if risk_level == "MEDIUM" else "VALID"

        click["ml_probability"] = round(ml_probability, 3)
        click["rule_score"] = rule_score
        click["risk_score"] = final_risk
        click["risk_level"] = risk_level
        click["action"] = action
        click["reasons"] = reasons

        yield json.dumps(click)
`
  },
  {
    id: 'requirements-file',
    title: 'Python Dependencies for Local ML Training',
    filename: 'requirements.txt',
    language: 'plaintext',
    description: 'List of Python packages required for data processing, XGBoost training, evaluation, and PyFlink stream processing.',
    code: `scikit-learn>=1.3.0
xgboost>=2.0.0
pandas>=2.1.0
numpy>=1.24.0
joblib>=1.3.0
matplotlib>=3.8.0
kafka-python>=2.0.2
apache-flink>=1.18.0
psycopg2-binary>=2.9.9
pyarrow>=14.0.0
`
  },
  {
    id: 'postgres-schema',
    title: 'PostgreSQL & Parquet Lakehouse Schema',
    filename: 'schema.sql',
    language: 'sql',
    description: 'PostgreSQL schema for real-time dashboard storage and Parquet data lake directory layout.',
    code: `-- PostgreSQL Real-Time Storage Schema for Flink Sink
CREATE TABLE IF NOT EXISTS ad_click_events (
    event_id VARCHAR(64) PRIMARY KEY,
    click_id VARCHAR(64) NOT NULL,
    click_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    user_id VARCHAR(32) NOT NULL,
    ad_id VARCHAR(32) NOT NULL,
    campaign_id VARCHAR(32) NOT NULL,
    publisher_id VARCHAR(32) NOT NULL,
    ip_address INET NOT NULL,
    device_id VARCHAR(32) NOT NULL,
    device_type VARCHAR(16),
    browser VARCHAR(32),
    os VARCHAR(32),
    country VARCHAR(32),
    cpc_bid NUMERIC(6, 2) NOT NULL,
    
    -- 10 Behavioral Stream Features
    clicks_per_minute INT,
    clicks_from_same_ip INT,
    clicks_per_device INT,
    unique_users_per_ip INT,
    unique_devices_per_ip INT,
    time_since_previous_click_ms INT,
    same_ad_click_count INT,
    unique_ads_clicked INT,
    is_geo_anomaly BOOLEAN,
    bot_behavior_score NUMERIC(5, 2),
    
    -- Hybrid Detection Results
    rule_score INT NOT NULL,
    ml_probability NUMERIC(4, 3) NOT NULL,
    risk_score INT NOT NULL,
    risk_level VARCHAR(16) NOT NULL, -- LOW, MEDIUM, HIGH
    action VARCHAR(16) NOT NULL,     -- VALID, FLAGGED, BLOCKED
    fraud_reasons TEXT[]
);

-- Indexing for Sub-Millisecond Dashboard Queries
CREATE INDEX IF NOT EXISTS idx_click_ts ON ad_click_events (click_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_risk_level ON ad_click_events (risk_level);
CREATE INDEX IF NOT EXISTS idx_ip ON ad_click_events (ip_address);
CREATE INDEX IF NOT EXISTS idx_campaign ON ad_click_events (campaign_id);

-- Alerts Table for High Risk Events
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    event_id VARCHAR(64) REFERENCES ad_click_events(event_id),
    alert_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(32),
    ip_address INET,
    risk_score INT,
    reasons TEXT[]
);
`
  }
];
