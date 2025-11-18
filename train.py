# trimport jsonain()
from pathlib import Path
import time
import tracemalloc
import warnings
import json
import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import randint, uniform
from imblearn.under_sampling import RandomUnderSampler

from imblearn.over_sampling import SMOTE

from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from xgboost import XGBRegressor
from mord import LogisticAT, OrdinalRidge
import joblib

warnings.filterwarnings("ignore")

# ----------------- Config -----------------
INPUT_CSV = "clean_database.csv"
TARGET = "chronic_kidney_disease_diagnosis"
ARTIFACT_DIR = Path("models")
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

#results of literature feature selection
COLS_SELECTED = [
    "age_years",
    "personal_history_dyslipidemia",
    "bmi_category",
    "abdominal_perimeter",
    "hdl_cholesterol_pathological",
    "weight_kg",
    "hba1c_elevated",
    "ldl_cholesterol_pathological",
    "family_history_overweight_obesity",
    "drug_consumption",
    "smoking_status",
    "alcohol_consumption",
    "exercise_30min_daily",
    "diet_frequency",
    "gender",
    "family_history_diabetes",
    "family_history_thyroid",
    "family_history_cardiac_hypertension",
    "family_history_cancer"
]

CLASS_TO_PROB = {0: 0.0, 1: 0.33, 2: 0.66, 3: 1.0}

# ----------------- Helpers -----------------
def get_top_features_anova(X, y, n=20):
    selector = SelectKBest(score_func=f_regression, k=n)
    selector.fit(X, y)
    mask = selector.get_support()
    top_features = X.columns[mask].tolist()
    scores = selector.scores_[mask]
    print("Top features by ANOVA F-test:")
    plt.figure(figsize=(10, 6))
    plt.barh(top_features, scores, color="skyblue")
    plt.xlabel("ANOVA F-test Score")
    plt.title("Top Features by ANOVA F-test")
    plt.tight_layout()
    plt.savefig(ARTIFACT_DIR / "anova_top_features.png")
    plt.close()
    return top_features


def apply_undersample(X_train, y_train, majority_class=0, min_samples=400):
    # Count classes
    class_counts = pd.Series(y_train).value_counts()
    sampling_strategy = {cls: count for cls, count in class_counts.items()}
    # Only reduce majority class if it exceeds min_samples
    if class_counts[majority_class] > min_samples:
        sampling_strategy[majority_class] = min_samples
    rus = RandomUnderSampler(sampling_strategy=sampling_strategy)
    X_res, y_res = rus.fit_resample(X_train, y_train)
    return X_res, y_res


def stratified_split_allowing_singletons(X, y, test_size=0.2):
    rng = np.random.RandomState()
    y = np.asarray(y)
    idx = np.arange(len(y))
    classes, counts = np.unique(y, return_counts=True)
    keep_classes = classes[counts >= 2]
    singleton_classes = classes[counts < 2]
    idx_keep = idx[np.isin(y, keep_classes)]
    idx_singleton = idx[np.isin(y, singleton_classes)]
    if len(idx_keep) == 0:
        return X, X.iloc[0:0].copy(), y, y[:0]
    X_keep = X.iloc[idx_keep]
    y_keep = y[idx_keep]
    X_tr_k, X_te_k, y_tr_k, y_te_k, idx_tr_k, idx_te_k = train_test_split(
        X_keep, y_keep, idx_keep,
        test_size=test_size, stratify=y_keep
    )
    X_train = pd.concat([X_tr_k, X.iloc[idx_singleton]], axis=0)
    y_train = np.concatenate([y_tr_k, y[idx_singleton]])
    X_test = X_te_k.copy()
    y_test = y_te_k.copy()
    perm = rng.permutation(len(X_train))
    X_train = X_train.iloc[perm]
    y_train = y_train[perm]
    return X_train, X_test, y_train, y_test


def apply_smote(X_train, y_train):
    sm = SMOTE()
    X_res, y_res = sm.fit_resample(X_train, y_train)
    return X_res, y_res


def rmse(y_true, y_pred):
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


# ----------------- Load & subset -----------------
def load_and_prepare_data():
    df = pd.read_csv(INPUT_CSV, index_col=0)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(axis=0, subset=[TARGET])

    missing = [c for c in COLS_SELECTED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(axis=0, subset=[TARGET])

    if "microalbuminuria_category" in df.columns:
        print("Cleaning microalbuminuria_category...")
        df["microalbuminuria_category"] = (
            df["microalbuminuria_category"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"A1": "A1", "A2": "A2", "0": "0"})
            .map({"0": 0, "A1": 1, "A2": 2})
        )

    df["microalbuminuria_category"] = df["microalbuminuria_category"].apply(
        lambda x: 0 if x == 0 else 1)
    y = df[TARGET].copy()
    X = df.drop(columns=[TARGET])
    X = X.drop(columns="gfr_proteinuria_relationship")

    for col in X.columns:
        if X[col].dtype == object:
            X[col] = X[col].astype(str).str.strip()
    return X, y


# ----------------- Preprocessing -----------------
def build_preprocessor(X_train):
    numeric_features = X_train.select_dtypes(
        include=[np.number]).columns.tolist()
    # Identify binary columns
    binary_features = [col for col in numeric_features if set(
        X_train[col].dropna().unique()) <= {0, 1}]
    # Continuous columns are numeric but not binary
    continuous_features = [
        col for col in numeric_features if col not in binary_features]
    categorical_features = [
        c for c in X_train.columns if c not in numeric_features]
    print("Continuous features:", continuous_features)
    print("Binary features:", binary_features)
    print("Categorical features:", categorical_features)
    # Preprocessing pipelines
    continuous_pre = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
    ])
    binary_pre = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        # No scaling for binary
    ])
    categorical_pre = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocess = ColumnTransformer(
        transformers=[
            ("cont", continuous_pre, continuous_features),
            ("bin", binary_pre, binary_features),
            ("cat", categorical_pre, categorical_features),
        ],
        remainder="drop",
    )
    return preprocess, continuous_features + binary_features, categorical_features


def spaces(preprocess):
    return {
        "LinearRegression": (
            Pipeline([("prep", preprocess), ("model", LinearRegression())]),
            {"model__fit_intercept": [True, False]}
        ),
        "Ridge": (
            Pipeline([("prep", preprocess), ("model", Ridge())]),
            {"model__alpha": uniform(1e-4, 100.0)}
        ),
        "Lasso": (
            Pipeline([("prep", preprocess), ("model", Lasso(max_iter=10000))]),
            {"model__alpha": uniform(1e-4, 10.0)}
        ),
        "XGBRegressor": (
            Pipeline([("prep", preprocess), ("model", XGBRegressor(
                n_jobs=-1, tree_method="hist",
                objective="reg:squarederror"
            ))]),
            {
                "model__max_depth": [3, 5, 7],
                "model__learning_rate": [0.01, 0.05, 0.1],
                "model__n_estimators": [100, 300, 500],
                "model__subsample": [0.6, 0.8],
                "model__colsample_bytree": [0.6, 0.8],
                "model__min_child_weight": [3, 5, 7],
                "model__gamma": [0, 1, 5],
                "model__reg_alpha": [0, 0.1, 1, 10],
                "model__reg_lambda": [0.1, 1, 10],
            }
        ),
        # "HistGBR": (
        #    Pipeline(
        #        [("prep", preprocess), ("model", HistGradientBoostingRegressor())]),
        #    {
        #        "model__max_iter": [100, 200, 300, 400, 500],
        #        "model__max_depth": [3, 5, 7, 10],
        #        "model__learning_rate": [0.01, 0.05, 0.1, 0.2],
        #        "model__l2_regularization": [1e-4, 1e-2, 0.1, 1.0],
        #    }
        # ),
        #"MLPRegressor": (
        #   Pipeline(
        #       [("prep", preprocess), ("model", MLPRegressor(max_iter=2000))]),
        #   {
        #       "model__hidden_layer_sizes": [
        #           (32,), (64,), (128,), (256,),
        #           (64, 32), (128, 64), (256, 128),
        #           (128, 64, 32), (256, 128, 64),
        #           (100,), (100, 50), (200, 100, 50)
        #       ],
        #       "model__activation": ["relu", "tanh", "logistic"],
        #       "model__solver": ["adam", "lbfgs", "sgd"],
        #       "model__alpha": [1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 0.1],
        #       "model__learning_rate": ["constant", "adaptive", "invscaling"],
        #       "model__learning_rate_init": [1e-4, 5e-4, 1e-3, 5e-3, 1e-2],
        #       "model__early_stopping": [True, False],
        #       "model__batch_size": ["auto", 32, 64, 128],
        #   }
        #),
        # "RandomForest": (
        #    Pipeline(
        #        [("prep", preprocess), ("model", RandomForestRegressor(n_jobs=-1))]),
        #    {
        #        "model__n_estimators": [100, 200, 300, 400],
        #        "model__max_depth": [None, 5, 10, 20],
        #        "model__min_samples_split": [2, 5, 10],
        #        "model__min_samples_leaf": [1, 2, 4],
        #        "model__max_features": ["sqrt", 0.5, 0.7, 1.0],
        #    }
        # ),
        "OrdinalRidge": (
            Pipeline([("prep", preprocess), ("model", OrdinalRidge())]),
            {"model__alpha": uniform(1e-4, 10.0)}
        ),
        "LogisticAT": (
            Pipeline([("prep", preprocess), ("model", LogisticAT())]),
            {"model__alpha": uniform(1e-4, 10.0)}
        ),
    }

# ----------------- Train & evaluate -----------------


def train_and_evaluate(X_train, X_test, y_train, y_test, preprocess):

    records = []
    all_cv_results = []
    best_name, best_estimator, best_score = None, None, -np.inf
    spaces_dict = spaces(preprocess)
    for name, (pipe, dist) in spaces_dict.items():
        print(f"\nSearching: {name}")
        tracemalloc.start()
        start_time = time.time()
        # Use integer classes for ordinal/classification models
        if name in ["OrdinalRidge", "LogisticAT"]:
            y_train_fit = y_train.astype(int)
            y_test_fit = y_test.astype(int)
        else:
            y_train_fit = y_train
            y_test_fit = y_test
        search = RandomizedSearchCV(
            estimator=pipe,
            param_distributions=dist,
            n_iter=50,
            cv=5,
            scoring="r2",
            n_jobs=-1,
            verbose=0,
        )
        search.fit(X_train, y_train_fit)
        elapsed = time.time() - start_time
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        m = search.best_estimator_
        y_pred = m.predict(X_test)
        r2 = r2_score(y_test_fit, y_pred)
        mae = mean_absolute_error(y_test_fit, y_pred)
        rmse_val = float(np.sqrt(mean_squared_error(y_test_fit, y_pred)))
        train_pred = m.predict(X_train)
        train_r2 = r2_score(y_train_fit, train_pred)
        rec = {
            "model": name,
            "r2_test": float(r2),
            "r2_train": float(train_r2),
            "mae": float(mae),
            "rmse": float(rmse_val),
            "best_params": search.best_params_,
            "train_time_sec": elapsed,
            "memory_peak_mb": peak / 1024 / 1024,
        }
        records.append(rec)
        # Guardar todos los resultados de la búsqueda
        cv_df = pd.DataFrame(search.cv_results_)
        cv_df["model"] = name
        all_cv_results.append(cv_df)
        if r2 > best_score:
            best_score = r2
            best_name = name
            best_estimator = m
    # Concatenar y guardar todos los resultados
    all_cv_df = pd.concat(all_cv_results, ignore_index=True)
    all_cv_df.to_csv(ARTIFACT_DIR / "all_cv_results.csv", index=False)
    return records, best_name, best_estimator, best_score


# ----------------- Persist artifacts -----------------
def persist_artifacts(best_estimator, best_name, records, best_score, class_counts, X_train, X_test, numeric_features, categorical_features):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = ARTIFACT_DIR / f"best_model_{timestamp}.joblib"
    joblib.dump(best_estimator, model_path)
    # Optionally, keep saving the latest as 'best_model.joblib' for convenience
    joblib.dump(best_estimator, ARTIFACT_DIR / "best_model.joblib")
    with open(ARTIFACT_DIR / "metrics.json", "w") as f:
        json.dump({
            "best_model": best_name,
            "results": records,
            "r2_best": float(best_score),
            "class_counts": class_counts.to_dict(),
            "train_size": int(len(X_train)),
            "test_size": int(len(X_test)),
        }, f, indent=2)
    schema = {
        "target": TARGET,
        "numeric": numeric_features,
        "categorical": categorical_features,
        "columns_selected": COLS_SELECTED,
    }
    with open(ARTIFACT_DIR / "feature_schema.json", "w") as f:
        json.dump(schema, f, indent=2)

def plot_top5_metrics(records, out_dir):
    df = pd.DataFrame(records)
    top5 = df.sort_values("r2_test", ascending=False).head(5)
    models = top5["model"]
    x = np.arange(len(models))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width/2, top5["r2_train"], width, label="Train R²")
    ax.bar(x + width/2, top5["r2_test"], width, label="Test R²")
    ax.set_xticks(x)
    ax.set_xticklabels(models, rotation=30)
    ax.set_ylabel("R² Score")
    ax.set_title("Top 5 Models by Test R²")
    ax.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "top5_r2.png")
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width/2, top5["rmse"], width, label="Test RMSE")
    ax.bar(x + width/2, top5["mae"], width, label="Test MAE")
    ax.set_xticks(x)
    ax.set_xticklabels(models, rotation=30)
    ax.set_ylabel("Error")
    ax.set_title("Top 5 Models: RMSE & MAE")
    ax.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "top5_errors.png")
    plt.close(fig)


def save_all_metadata(records, out_dir):
    df = pd.DataFrame(records)
    df.to_csv(out_dir / "all_runs.csv", index=False)


def merge_rare_classes(y, rare_class=4, target_class=3):
    y = y.copy()
    y[y == rare_class] = target_class
    return y

# ----------------- Main -----------------


def main():
    X, y = load_and_prepare_data()
    y = merge_rare_classes(y, rare_class=4, target_class=3)

    N = 25
    top_features = get_top_features_anova(X, y, n=N)
    X_selected = X[top_features]
    class_counts = pd.Series(y).value_counts().sort_index()
    print("Class distribution:")
    for cls, count in class_counts.items():
        print(f"  Class {cls}: {count} samples")
    X_train, X_test, y_train, y_test = stratified_split_allowing_singletons(
        X_selected, y, test_size=0.25
    )

    X_train, y_train = apply_undersample(X_train, y_train)
    X_train, y_train = apply_smote(X_train, y_train)

    print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")
    preprocess, numeric_features, categorical_features = build_preprocessor(
        X_train)

    y_train = pd.Series(y_train).map(CLASS_TO_PROB)
    y_test = pd.Series(y_test).map(CLASS_TO_PROB)

    records, best_name, best_estimator, best_score = train_and_evaluate(
        X_train, X_test, y_train, y_test, preprocess
    )
    persist_artifacts(
        best_estimator, best_name, records, best_score, class_counts,
        X_train, X_test, numeric_features, categorical_features
    )
    with open(ARTIFACT_DIR / "metrics.json") as f:
        metrics = json.load(f)
    df_metrics = pd.DataFrame(metrics["results"])
    table = df_metrics[["model", "train_time_sec",
                        "memory_peak_mb", "r2_test", "rmse"]]
    print(table.to_markdown(index=False))
    print(f"\nSaved best model: {best_name} (R2={best_score:.4f}) → {
          ARTIFACT_DIR/'best_model.joblib'}")
    print("Metrics written to:", ARTIFACT_DIR / "metrics.json")
    print("Feature schema written to:", ARTIFACT_DIR / "feature_schema.json")
    plot_top5_metrics(records, ARTIFACT_DIR)
    save_all_metadata(records, ARTIFACT_DIR)
    print("Top 5 metrics plots saved to:", ARTIFACT_DIR)
    print("All run metadata saved to:", ARTIFACT_DIR / "all_runs.csv")


if __name__ == "__main__":
    main()
