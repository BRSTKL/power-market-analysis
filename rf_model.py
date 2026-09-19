"""
rf_model.py - Random Forest Model for Energy Price Prediction

This module builds a Random Forest regression model to predict
German electricity prices (EUR/MWh) using time-based and lag features
derived from SMARD API data.
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for Windows compatibility
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from market_data import fetch_smard_data


# -- Feature Engineering ------------------------------------------------------

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate time-based and lag features from the price DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'Datetime' and 'Price_EUR_MWh' columns.

    Returns
    -------
    pd.DataFrame
        DataFrame enriched with engineered features.
    """
    result = df.copy()

    # Time-based features
    result["hour"] = result["Datetime"].dt.hour
    result["day_of_week"] = result["Datetime"].dt.dayofweek       # 0=Mon, 6=Sun
    result["is_weekend"] = (result["day_of_week"] >= 5).astype(int)

    # Cyclical encoding for hour (captures 0-23 wrap-around)
    result["hour_sin"] = np.sin(2 * np.pi * result["hour"] / 24)
    result["hour_cos"] = np.cos(2 * np.pi * result["hour"] / 24)

    # Lag features (previous hours' prices)
    for lag in [1, 2, 3, 6, 12, 24]:
        result[f"lag_{lag}h"] = result["Price_EUR_MWh"].shift(lag)

    # Rolling statistics
    result["rolling_mean_6h"] = result["Price_EUR_MWh"].rolling(6).mean()
    result["rolling_std_6h"] = result["Price_EUR_MWh"].rolling(6).std()
    result["rolling_mean_24h"] = result["Price_EUR_MWh"].rolling(24).mean()
    result["rolling_std_24h"] = result["Price_EUR_MWh"].rolling(24).std()

    # Price change (momentum)
    result["price_change_1h"] = result["Price_EUR_MWh"].diff(1)
    result["price_change_6h"] = result["Price_EUR_MWh"].diff(6)

    # Drop rows with NaN created by lag/rolling operations
    result.dropna(inplace=True)
    result.reset_index(drop=True, inplace=True)

    return result


# -- Model Training & Evaluation ----------------------------------------------

FEATURE_COLUMNS = [
    "hour", "day_of_week", "is_weekend",
    "hour_sin", "hour_cos",
    "lag_1h", "lag_2h", "lag_3h", "lag_6h", "lag_12h", "lag_24h",
    "rolling_mean_6h", "rolling_std_6h",
    "rolling_mean_24h", "rolling_std_24h",
    "price_change_1h", "price_change_6h",
]

TARGET_COLUMN = "Price_EUR_MWh"


def train_model(
    df: pd.DataFrame,
    test_size: float = 0.2,
    n_estimators: int = 100,
    random_state: int = 42,
) -> dict:
    """
    Train a Random Forest model and return results.

    Parameters
    ----------
    df : pd.DataFrame
        Feature-engineered DataFrame from create_features().
    test_size : float
        Fraction of data to use for testing.
    n_estimators : int
        Number of trees in the forest.
    random_state : int
        Random seed for reproducibility.

    Returns
    -------
    dict
        Contains 'model', 'metrics', 'predictions', 'X_test', 'y_test',
        'feature_importance', and 'datetimes_test'.
    """
    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]
    datetimes = df["Datetime"]

    # Chronological split (no shuffle) to respect time-series order
    split_idx = int(len(df) * (1 - test_size))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    dt_test = datetimes.iloc[split_idx:]

    # Train Random Forest
    model = RandomForestRegressor(
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)

    # Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # Feature importance
    importance = pd.DataFrame({
        "feature": FEATURE_COLUMNS,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False).reset_index(drop=True)

    return {
        "model": model,
        "metrics": {"MAE": round(mae, 2), "RMSE": round(rmse, 2), "R2": round(r2, 4)},
        "predictions": y_pred,
        "X_test": X_test,
        "y_test": y_test.values,
        "datetimes_test": dt_test.values,
        "feature_importance": importance,
    }


# -- Visualization -------------------------------------------------------------

def plot_results(results: dict, save_path: str = "rf_results.png") -> None:
    """
    Generate a 3-panel chart: Actual vs Predicted, Residuals, Feature Importance.

    Parameters
    ----------
    results : dict
        Output from train_model().
    save_path : str
        File path to save the plot.
    """
    y_test = results["y_test"]
    y_pred = results["predictions"]
    dt_test = results["datetimes_test"]
    importance = results["feature_importance"]
    metrics = results["metrics"]

    fig, axes = plt.subplots(3, 1, figsize=(14, 12), constrained_layout=True)
    fig.suptitle(
        "Random Forest - Electricity Price Prediction (Germany)",
        fontsize=15, fontweight="bold",
    )

    # 1. Actual vs Predicted
    ax1 = axes[0]
    ax1.plot(dt_test, y_test, label="Actual", color="#2196F3", linewidth=1.2)
    ax1.plot(dt_test, y_pred, label="Predicted", color="#FF5722", linewidth=1.2, linestyle="--")
    ax1.set_ylabel("Price (EUR/MWh)")
    ax1.set_title(
        f"Actual vs Predicted  |  MAE={metrics['MAE']}  RMSE={metrics['RMSE']}  R2={metrics['R2']}"
    )
    ax1.legend()
    ax1.grid(alpha=0.3)

    # 2. Residuals
    ax2 = axes[1]
    residuals = y_test - y_pred
    ax2.bar(range(len(residuals)), residuals, color="#9C27B0", alpha=0.6, width=1.0)
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_ylabel("Residual (EUR/MWh)")
    ax2.set_title("Prediction Residuals")
    ax2.grid(alpha=0.3)

    # 3. Feature Importance
    ax3 = axes[2]
    colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(importance)))
    ax3.barh(importance["feature"], importance["importance"], color=colors)
    ax3.set_xlabel("Importance")
    ax3.set_title("Feature Importance")
    ax3.invert_yaxis()
    ax3.grid(alpha=0.3, axis="x")

    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    print(f"\n[PLOT] Results saved to: {save_path}")
    plt.close()


# -- Entry Point ---------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Random Forest - Energy Price Prediction")
    print("=" * 60)

    # 1. Fetch data
    print("\n[1/4] Fetching data from SMARD API...")
    raw_df = fetch_smard_data()

    if raw_df is None:
        print("Failed to fetch data. Exiting.")
        exit(1)

    # 2. Feature engineering
    print("\n[2/4] Engineering features...")
    featured_df = create_features(raw_df)
    print(f"  Samples after feature engineering: {len(featured_df)}")
    print(f"  Features: {len(FEATURE_COLUMNS)}")

    # 3. Train model
    print("\n[3/4] Training Random Forest model...")
    results = train_model(featured_df, test_size=0.2, n_estimators=200)

    print("\n[METRICS] Model Performance:")
    for metric, value in results["metrics"].items():
        print(f"   {metric:>6}: {value}")

    print("\n[FEATURES] Top 5 Important Features:")
    print(results["feature_importance"].head().to_string(index=False))

    # 4. Plot results
    print("\n[4/4] Generating plots...")
    plot_results(results, save_path="rf_results.png")

    print("\n" + "=" * 60)
    print("  Prediction complete.")
    print("=" * 60)
