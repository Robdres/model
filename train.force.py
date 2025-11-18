import pandas as pd
import numpy as np
import itertools
import random
from pathlib import Path
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

# CONFIG
INPUT_CSV = "clean_database.csv"
TARGET = "chronic_kidney_disease_diagnosis"
RESULTS_CSV = "feature_combo_results.csv"
N_FEATURES = 24
# Number of combinations to test (increase for more exhaustive search)
N_COMBOS = 50


def build_preprocessor(X_train):
    numeric_features = X_train.select_dtypes(
        include=[np.number]).columns.tolist()
    categorical_features = [
        c for c in X_train.columns if c not in numeric_features]
    numeric_pre = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
    ])
    categorical_pre = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocess = ColumnTransformer([
        ("num", numeric_pre, numeric_features),
        ("cat", categorical_pre, categorical_features),
    ], remainder="drop")
    return preprocess


def evaluate_combo(X, y, feature_list):
    X_sel = X[feature_list]
    X_train, X_test, y_train, y_test = train_test_split(
        X_sel, y, test_size=0.2, random_state=RANDOM_SEED
    )
    preprocess = build_preprocessor(X_train)
    pipe = Pipeline([
        ("prep", preprocess),
        ("model", XGBRegressor(
            n_jobs=-1, tree_method="hist", objective="reg:squarederror", random_state=RANDOM_SEED
        ))
    ])
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    return r2, mae, rmse


def main():
    df = pd.read_csv(INPUT_CSV, index_col=0)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(axis=0, subset=[TARGET])
    y = df[TARGET].copy()
    X = df.drop(columns=[TARGET])
    for col in X.columns:
        if X[col].dtype == object:
            # Check if column contains values like 'number/number'
            if X[col].str.contains(r'^\d+/\d+$').any():
                # Create two new columns
                X[[f"{col}_systolic", f"{col}_diastolic"]] = X[col].str.split('/', expand=True).astype(float)
                X = X.drop(columns=[col])
            else:
                X[col] = X[col].astype(str).str.strip()
    all_features = X.columns.tolist()

    combos = []
    # Generate random combinations of N_FEATURES
    for _ in range(N_COMBOS):
        combo = random.sample(all_features, N_FEATURES)
        combos.append(combo)

    results = []
    for i, feature_list in enumerate(combos):
        print(f"Testing combo {i+1}/{N_COMBOS}")
        try:
            r2, mae, rmse = evaluate_combo(X, y, feature_list)
            results.append({
                "combo_id": i+1,
                "features": ";".join(feature_list),
                "r2": r2,
                "mae": mae,
                "rmse": rmse
            })
        except Exception as e:
            print(f"Error with combo {i+1}: {e}")
            results.append({
                "combo_id": i+1,
                "features": ";".join(feature_list),
                "r2": None,
                "mae": None,
                "rmse": None,
                "error": str(e)
            })

    results_df = pd.DataFrame(results)
    results_df.to_csv(RESULTS_CSV, index=False)
    print(f"Results saved to {RESULTS_CSV}")


if __name__ == "__main__":
    main()
