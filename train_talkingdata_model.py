"""
FlinkGuard - Realtime Ad Click Fraud Detection
ML Training Pipeline: Kaggle TalkingData AdTracking Fraud Dataset
GPU-Accelerated Model Training with NVIDIA CUDA Support (RTX 4050)
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timezone

# Ensure clean UTF-8 output on Windows terminal
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass
import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
    average_precision_score,
    precision_score,
    recall_score,
    f1_score
)
from sklearn.ensemble import RandomForestClassifier
import xgboost as xgb

def parse_args():
    parser = argparse.ArgumentParser(description="Train Click Fraud Detection Model on Kaggle TalkingData")
    parser.add_argument(
        "--data_path",
        type=str,
        default="data/train_sample.csv",
        help="Path to TalkingData CSV (e.g., data/train_sample.csv or data/train.csv)"
    )
    parser.add_argument(
        "--nrows",
        type=int,
        default=500000,
        help="Number of rows to load (use -1 to load entire dataset, default 500,000)"
    )
    parser.add_argument(
        "--use_gpu",
        action="store_true",
        default=True,
        help="Enable NVIDIA GPU acceleration (CUDA)"
    )
    parser.add_argument(
        "--n_estimators",
        type=int,
        default=250,
        help="Number of gradient boosted trees"
    )
    parser.add_argument(
        "--max_depth",
        type=int,
        default=7,
        help="Maximum depth of trees"
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        default=0.08,
        help="Learning rate (eta)"
    )
    return parser.parse_args()

def load_data(file_path: str, nrows: int = None) -> pd.DataFrame:
    print(f"\n[1/5] Loading TalkingData dataset from '{file_path}'...")
    if not os.path.exists(file_path):
        # Check if user specified relative to workspace
        alt_path = os.path.join("data", file_path)
        if os.path.exists(alt_path):
            file_path = alt_path
        else:
            raise FileNotFoundError(f"Dataset file not found at: {file_path}")

    start_t = time.time()
    read_kwargs = {
        'parse_dates': ['click_time'],
        'dtype': {
            'ip': 'uint32',
            'app': 'uint16',
            'device': 'uint16',
            'os': 'uint16',
            'channel': 'uint16',
            'is_attributed': 'uint8'
        }
    }
    if nrows and nrows > 0:
        read_kwargs['nrows'] = nrows
        print(f"      Reading first {nrows:,} records...")
    else:
        print("      Reading full dataset...")

    df = pd.read_csv(file_path, **read_kwargs)
    elapsed = time.time() - start_t
    print(f"      [OK] Loaded {len(df):,} rows in {elapsed:.2f}s ({df.memory_usage().sum() / 1e6:.1f} MB in RAM)")
    return df

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[2/5] Engineering 10 Behavioral Streaming Features (Flink Window Emulation)...")
    start_t = time.time()

    # Chronological sort for strict temporal validity
    df = df.sort_values(by=['click_time']).reset_index(drop=True)

    # Time features
    df['hour'] = df['click_time'].dt.hour.astype('uint8')
    df['minute'] = df['click_time'].dt.minute.astype('uint8')
    df['second'] = df['click_time'].dt.second.astype('uint8')
    df['epoch_sec'] = (df['click_time'].astype('int64') // 10**9).astype('int64')

    print("      -> Calculating IP click frequency and device diversity...")
    # Feature 1: IP click frequency
    df['clicks_from_same_ip'] = df.groupby('ip')['click_time'].transform('count').astype('uint32')

    # Feature 2: Clicks per device type
    df['clicks_per_device'] = df.groupby('device')['click_time'].transform('count').astype('uint32')

    # Feature 3 & 4: Unique devices and channels per IP (Bot-farm / proxy detection)
    df['unique_devices_per_ip'] = df.groupby('ip')['device'].transform('nunique').astype('uint16')
    df['unique_channels_per_ip'] = df.groupby('ip')['channel'].transform('nunique').astype('uint16')

    # Feature 5: Delta time since previous click from same IP (Velocity delta)
    print("      -> Calculating inter-click velocity deltas...")
    df['prev_click_epoch'] = df.groupby('ip')['epoch_sec'].shift(1)
    df['time_since_previous_click'] = (df['epoch_sec'] - df['prev_click_epoch']).fillna(9999).clip(0, 9999).astype('float32')
    df.drop(columns=['prev_click_epoch'], inplace=True)

    # Feature 6: Same app click frequency per IP
    df['same_app_click_count'] = df.groupby(['ip', 'app'])['click_time'].transform('count').astype('uint32')

    # Feature 7: Unique apps per IP
    df['unique_apps_per_ip'] = df.groupby('ip')['app'].transform('nunique').astype('uint16')

    # Feature 8: Group combination frequency (ip + app + channel)
    df['ip_app_channel_frequency'] = df.groupby(['ip', 'app', 'channel'])['click_time'].transform('count').astype('uint32')

    # Feature 9: Group combination frequency (ip + device + os)
    df['ip_device_os_frequency'] = df.groupby(['ip', 'device', 'os'])['click_time'].transform('count').astype('uint32')

    # Feature 10: Bot Behavior Ratio
    df['bot_behavior_ratio'] = (df['same_app_click_count'] / (df['unique_apps_per_ip'] + 1e-5)).round(2).astype('float32')

    # Target variable setup:
    # In TalkingData, is_attributed=1 represents genuine conversions, is_attributed=0 represents non-converting / fraud clicks
    # Click Fraud Label: is_fraud = (1 - is_attributed) or target based on non-conversion anomaly
    if 'is_attributed' in df.columns:
        df['is_fraud'] = (1 - df['is_attributed']).astype('uint8')

    elapsed = time.time() - start_t
    print(f"      [OK] Completed feature engineering in {elapsed:.2f}s.")
    return df

def train_model(df: pd.DataFrame, args):
    feature_cols = [
        'app', 'device', 'os', 'channel', 'hour',
        'clicks_from_same_ip', 'clicks_per_device',
        'unique_devices_per_ip', 'unique_channels_per_ip',
        'time_since_previous_click', 'same_app_click_count',
        'unique_apps_per_ip', 'ip_app_channel_frequency',
        'ip_device_os_frequency', 'bot_behavior_ratio'
    ]
    target_col = 'is_fraud'

    X = df[feature_cols]
    y = df[target_col]

    fraud_count = int(y.sum())
    legit_count = int(len(y) - fraud_count)
    print(f"\n[3/5] Dataset Class Distribution:")
    print(f"      Non-Attributed / Fraud: {fraud_count:,} ({fraud_count / len(y) * 100:.2f}%)")
    print(f"      Attributed / Legit:     {legit_count:,} ({legit_count / len(y) * 100:.2f}%)")

    # Split 80% train, 20% test (chronological split)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print(f"      Train set: {len(X_train):,} rows | Test set: {len(X_test):,} rows")

    # XGBoost Parameters with GPU acceleration
    scale_pos_weight = max(1.0, legit_count / (fraud_count + 1e-5))

    xgb_params = {
        'n_estimators': args.n_estimators,
        'max_depth': args.max_depth,
        'learning_rate': args.learning_rate,
        'scale_pos_weight': scale_pos_weight,
        'subsample': 0.85,
        'colsample_bytree': 0.85,
        'random_state': 42,
        'eval_metric': 'auc'
    }

    if args.use_gpu:
        try:
            xgb_params['tree_method'] = 'hist'
            xgb_params['device'] = 'cuda'
            print("\n[4/5] Training XGBoost with NVIDIA GPU Acceleration (CUDA / RTX 4050)...")
        except Exception as e:
            print(f"      [!] GPU init fallback: {e}. Using CPU 'hist'.")
            xgb_params['tree_method'] = 'hist'
    else:
        xgb_params['tree_method'] = 'hist'
        print("\n[4/5] Training XGBoost on CPU (hist)...")

    model = xgb.XGBClassifier(**xgb_params)
    train_start = time.time()
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_test, y_test)],
        verbose=max(1, args.n_estimators // 5)
    )
    train_time = time.time() - train_start
    print(f"      [OK] Model training finished in {train_time:.2f}s.")

    # Predictions & Evaluation
    print("\n[5/5] Evaluating Model Performance...")
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_pred_proba >= 0.50).astype(int)

    roc_auc = float(roc_auc_score(y_test, y_pred_proba))
    pr_auc = float(average_precision_score(y_test, y_pred_proba))
    prec = float(precision_score(y_test, y_pred, zero_division=0))
    rec = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    cm = confusion_matrix(y_test, y_pred).tolist()

    print("\n" + "="*58)
    print("           MODEL EVALUATION SUMMARY RESULTS")
    print("="*58)
    print(f"  ROC-AUC Score:             {roc_auc:.4f}")
    print(f"  PR-AUC (Avg Precision):    {pr_auc:.4f}")
    print(f"  Precision:                 {prec:.4f}")
    print(f"  Recall:                    {rec:.4f}")
    print(f"  F1-Score:                  {f1:.4f}")
    print(f"  Confusion Matrix:          {cm}")
    print("="*58)

    # Feature Importances
    importances = model.feature_importances_
    feat_imp = sorted(zip(feature_cols, importances), key=lambda x: x[1], reverse=True)
    print("\n  Top Predictive Features:")
    for f_name, imp in feat_imp:
        print(f"    - {f_name:30s}: {imp * 100:.2f}%")

    # Persist Artifacts
    os.makedirs("models", exist_ok=True)
    model_pkl_path = "models/fraud_model.pkl"
    model_json_path = "models/fraud_model.json"
    metrics_path = "models/model_metrics.json"

    artifacts = {
        'model': model,
        'feature_names': feature_cols,
        'trained_at': datetime.now(timezone.utc).isoformat(),
        'optimal_threshold': 0.60,
        'roc_auc': roc_auc,
        'pr_auc': pr_auc,
        'f1_score': f1
    }
    joblib.dump(artifacts, model_pkl_path)
    model.save_model(model_json_path)

    metrics_dict = {
        'roc_auc': roc_auc,
        'pr_auc': pr_auc,
        'precision': prec,
        'recall': rec,
        'f1_score': f1,
        'confusion_matrix': cm,
        'training_time_seconds': train_time,
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'feature_importances': {f_name: float(imp) for f_name, imp in feat_imp},
        'feature_names': feature_cols,
        'updated_at': datetime.now(timezone.utc).isoformat()
    }
    with open(metrics_path, "w") as f:
        json.dump(metrics_dict, f, indent=2)

    print(f"\n[OK] Saved artifacts:")
    print(f"    - {model_pkl_path} (Joblib Model Bundle)")
    print(f"    - {model_json_path} (Native XGBoost Model)")
    print(f"    - {metrics_path} (Metrics & Feature Importances)")
    print("[OK] Pipeline finished successfully!\n")

def main():
    args = parse_args()
    print("="*58)
    print("  FlinkGuard: TalkingData Ad-Click Fraud ML Pipeline")
    print("  GPU: NVIDIA GeForce RTX 4050 (CUDA)")
    print("="*58)

    df = load_data(args.data_path, args.nrows)
    df = engineer_features(df)
    train_model(df, args)

if __name__ == "__main__":
    main()
