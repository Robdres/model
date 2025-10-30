# train_all_models_with_singleton_safe_split.py
import json, warnings
from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd
from scipy.stats import randint, uniform

from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from xgboost import XGBRegressor

import joblib
warnings.filterwarnings("ignore")

# ----------------- Config -----------------
INPUT_CSV = "clean_database.csv"
TARGET = "chronic_kidney_disease_diagnosis"
ARTIFACT_DIR = Path("models")
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
RANDOM_STATE = 12

# Your selected columns
COLS_SELECTED = [
    "chronic_kidney_disease_diagnosis", "age_years", "personal_history_dyslipidemia",
    "bmi_category", "abdominal_perimeter", "hdl_cholesterol_pathological", "weight_kg",
     "hba1c_elevated",
     "ldl_cholesterol_pathological",
    "family_history_overweight_obesity",
    "drug_consumption", "smoking_status", "alcohol_consumption",
    "exercise_30min_daily", "diet_frequency", "gender", "family_history_diabetes",
    "family_history_thyroid", "family_history_cardiac_hypertension",
    "family_history_cancer"
]

# If the target came as {0, 0.4, 0.6, 0.8, 1}, map back to {0..4}
FLOAT_TO_CLASS = {0.0: 0, 0.4: 1, 0.6: 2, 0.8: 3, 1.0: 4}

# ----------------- Helpers -----------------
def coerce_target_to_classes(y_series: pd.Series) -> np.ndarray:
    """
    Map 0/0.4/0.6/0.8/1.0 or already-0..4 into integer classes 0..4.
    """
    s = pd.to_numeric(y_series, errors="coerce")
    mapped = s.map(FLOAT_TO_CLASS)
    if mapped.isna().any():
        # Try snapping floats in [0,1] to nearest ladder step
        ladder = np.array(sorted(FLOAT_TO_CLASS.keys()))
        mask = mapped.isna()
        vals = s[mask].to_numpy()
        if len(vals) > 0:
            idx = np.abs(vals[:, None] - ladder[None, :]).argmin(axis=1)
            mapped.loc[mask] = ladder[idx]
        mapped = mapped.map(FLOAT_TO_CLASS)
    if mapped.isna().any():
        intish = s.round().astype("Int64")
        if not set(intish.dropna().unique()).issubset({0,1,2,3,4}):
            raise ValueError("Unexpected TARGET values; cannot map to {0..4}.")
        mapped = intish.astype(int)
    return mapped.astype(int).to_numpy()

def stratified_split_allowing_singletons(X, y, test_size=0.2, random_state=42):
    """
    Stratified split for ordinal classes where some classes may have <2 samples.
    - Any class with <2 samples is kept entirely in TRAIN.
    - Remaining classes are split with stratify=y.
    Returns: X_train, X_test, y_train, y_test
    """
    rng = np.random.RandomState(random_state)
    y = np.asarray(y)
    idx = np.arange(len(y))
    classes, counts = np.unique(y, return_counts=True)
    keep_classes = classes[counts >= 2]
    singleton_classes = classes[counts < 2]

    idx_keep = idx[np.isin(y, keep_classes)]
    idx_singleton = idx[np.isin(y, singleton_classes)]

    if len(idx_keep) == 0:
        # No class has >=2 samples → everything goes to train; empty test
        return X, X.iloc[0:0].copy(), y, y[:0]

    X_keep = X.iloc[idx_keep]
    y_keep = y[idx_keep]

    # Keep track of original indices for a clean concat later
    X_tr_k, X_te_k, y_tr_k, y_te_k, idx_tr_k, idx_te_k = train_test_split(
        X_keep, y_keep, idx_keep,
        test_size=test_size, random_state=random_state, stratify=y_keep
    )

    # Add all singletons to TRAIN
    X_train = pd.concat([X_tr_k, X.iloc[idx_singleton]], axis=0)
    y_train = np.concatenate([y_tr_k, y[idx_singleton]])
    X_test = X_te_k.copy()
    y_test = y_te_k.copy()

    # Shuffle train to mix in the singletons
    perm = rng.permutation(len(X_train))
    X_train = X_train.iloc[perm]
    y_train = y_train[perm]

    return X_train, X_test, y_train, y_test

def rmse(y_true, y_pred):
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))

# ----------------- Load & subset -----------------
df = pd.read_csv(INPUT_CSV, index_col=0)
missing = [c for c in COLS_SELECTED if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns in CSV: {missing}")

df = df[COLS_SELECTED].copy()
df = df.replace([np.inf, -np.inf], np.nan)

# Coerce target to ordinal classes 0..4 and drop missing targets
df = df.dropna(axis=0, subset=[TARGET])
y = coerce_target_to_classes(df[TARGET])
X = df.drop(columns=[TARGET])

# (Optional) Light clean-up on object cols: strip text
for col in X.columns:
    if X[col].dtype == object:
        X[col] = X[col].astype(str).str.strip()

# Report class counts
class_counts = pd.Series(y).value_counts().sort_index()
print("Class counts:", dict(class_counts))

# ----------------- Split (singleton-safe) -----------------
X_train, X_test, y_train, y_test = stratified_split_allowing_singletons(
    X, y, test_size=0.2, random_state=RANDOM_STATE
)
print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")

# ----------------- Preprocessing -----------------
numeric_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
categorical_features = [c for c in X_train.columns if c not in numeric_features]

numeric_pre = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler(with_mean=True, with_std=True)),
])

categorical_pre = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
])

preprocess = ColumnTransformer(
    transformers=[
        ("num", numeric_pre, numeric_features),
        ("cat", categorical_pre, categorical_features),
    ],
    remainder="drop",
)

def spaces():
    return {
        "LinearRegression": (
            Pipeline([("prep", preprocess), ("model", LinearRegression())]),
            {"model__fit_intercept": [True, False]}
        ),
        "Ridge": (
            Pipeline([("prep", preprocess), ("model", Ridge(random_state=RANDOM_STATE))]),
            {"model__alpha": uniform(1e-4, 100.0)}
        ),
        "Lasso": (
            Pipeline([("prep", preprocess), ("model", Lasso(random_state=RANDOM_STATE, max_iter=10000))]),
            {"model__alpha": uniform(1e-4, 10.0)}
        ),
        "RandomForest": (
            Pipeline([("prep", preprocess), ("model", RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=-1))]),
            {
                "model__n_estimators": randint(200, 700),
                "model__max_depth": [None] + list(range(3, 16)),
                "model__min_samples_split": randint(2, 12),
                "model__min_samples_leaf": randint(1, 10),
                "model__max_features": ["sqrt", 0.5, 0.7, 1.0],
            }
        ),
        "XGBRegressor": (
            Pipeline([("prep", preprocess), ("model", XGBRegressor(
                random_state=RANDOM_STATE, n_jobs=-1, tree_method="hist",
                objective="reg:squarederror"
            ))]),
            {
                "model__n_estimators": randint(300, 900),
                "model__max_depth": randint(3, 10),
                "model__learning_rate": uniform(0.01, 0.29),
                "model__subsample": uniform(0.6, 0.4),
                "model__colsample_bytree": uniform(0.6, 0.4),
                "model__min_child_weight": randint(1, 10),
            }
        ),
        "MLPRegressor": (
            Pipeline([("prep", preprocess),
                      ("model", MLPRegressor(random_state=RANDOM_STATE, max_iter=1000))]),
            {
                "model__hidden_layer_sizes": [(64,), (128,), (128, 64), (256, 128)],
                "model__activation": ["relu", "tanh"],
                "model__alpha": uniform(1e-6, 1e-2),
                "model__learning_rate_init": uniform(1e-4, 1e-2),
            }
        ),
    }

# ----------------- Train & evaluate -----------------
records = []
best_name, best_estimator, best_score = None, None, -np.inf

for name, (pipe, dist) in spaces().items():
    print(f"\nSearching: {name}")
    search = RandomizedSearchCV(
        estimator=pipe,
        param_distributions=dist,
        n_iter=30,
        cv=5,
        scoring="r2",
        n_jobs=-1,
        random_state=RANDOM_STATE,
        verbose=0,
    )
    search.fit(X_train, y_train)
    m = search.best_estimator_

    y_pred = m.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse_val = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    train_r2 = r2_score(y_train, m.predict(X_train))

    rec = {
        "model": name,
        "r2_test": float(r2),
        "r2_train": float(train_r2),
        "mae": float(mae),
        "rmse": float(rmse_val),
        "best_params": search.best_params_,
    }
    records.append(rec)

    if r2 > best_score:
        best_score = r2
        best_name = name
        best_estimator = m

# ----------------- Persist artifacts -----------------
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

print(f"\nSaved best model: {best_name} (R2={best_score:.4f}) → {ARTIFACT_DIR/'best_model.joblib'}")
print("Metrics written to:", ARTIFACT_DIR / "metrics.json")
print("Feature schema written to:", ARTIFACT_DIR / "feature_schema.json")

