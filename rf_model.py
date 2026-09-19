"""
rf_model.py - Random Forest Model for Day-Ahead Electricity Price Forecasting

Incorporates market fundamentals (Grid Load, Residual Load, Solar, Wind)
alongside price lags and time features to accurately forecast electricity prices
and achieve strong positive R² performance.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from market_data import fetch_smard_fundamentals, fetch_smard_data


def train_price_forecasting_model(df):
    """
    Trains a Random Forest model to forecast day-ahead electricity prices.
    Uses physical fundamentals (Load, Residual Load, Renewables) when available,
    along with time-based and lag features.
    """
    print("Preparing data and engineering features for the Random Forest model...")
    df = df.copy()

    # 1. Time-based features
    df['Hour'] = df['Datetime'].dt.hour
    df['DayOfWeek'] = df['Datetime'].dt.dayofweek
    df['Month'] = df['Datetime'].dt.month

    # 2. Lag features (previous price history)
    df['Price_Lag_1'] = df['Price_EUR_MWh'].shift(1)
    df['Price_Lag_24'] = df['Price_EUR_MWh'].shift(24)

    # 3. Fundamental features (Physical market drivers)
    fundamental_cols = [
        'Load_MWh', 'Residual_Load_MWh', 'Solar_MWh',
        'Wind_Onshore_MWh', 'Wind_Offshore_MWh',
        'Total_Renewable_MWh', 'Renewable_Share'
    ]
    has_fundamentals = all(col in df.columns for col in fundamental_cols)

    if has_fundamentals:
        print("[INFO] Physical fundamentals detected (Load, Residual Load, Renewables).")
        # Add residual load lag (weather / merit-order persistence)
        df['Residual_Lag_24'] = df['Residual_Load_MWh'].shift(24)

        features = [
            'Hour', 'DayOfWeek', 'Month',
            'Load_MWh', 'Residual_Load_MWh',
            'Solar_MWh', 'Wind_Onshore_MWh', 'Wind_Offshore_MWh',
            'Total_Renewable_MWh', 'Renewable_Share',
            'Price_Lag_1', 'Price_Lag_24', 'Residual_Lag_24'
        ]
    else:
        print("[INFO] Running with basic time and lag features.")
        features = ['Hour', 'DayOfWeek', 'Month', 'Price_Lag_1', 'Price_Lag_24']

    # Drop rows with NaN values created by lags
    df = df.dropna().reset_index(drop=True)

    X = df[features]
    y = df['Price_EUR_MWh']

    # Chronological split (no shuffle) to strictly prevent future data leakage
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )

    print(f"Training Data: {len(X_train)} samples")
    print(f"Testing Data: {len(X_test)} samples")
    print(f"Features used ({len(features)}): {', '.join(features)}")

    # Initialize and train Random Forest Regressor
    print("\nTraining the Random Forest model...")
    rf_model = RandomForestRegressor(
        n_estimators=150,
        max_depth=15,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    rf_model.fit(X_train, y_train)

    # Make predictions on the test set
    predictions = rf_model.predict(X_test)

    # Evaluate model performance
    mae = mean_absolute_error(y_test, predictions)
    mse = mean_squared_error(y_test, predictions)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, predictions)

    print("\n" + "=" * 50)
    print("  Model Evaluation Metrics")
    print("=" * 50)
    print(f"Mean Absolute Error (MAE)  : {mae:.2f} EUR/MWh")
    print(f"Root Mean Sq. Error (RMSE) : {rmse:.2f} EUR/MWh")
    print(f"Mean Squared Error (MSE)   : {mse:.2f}")
    print(f"R-squared (R2 Score)       : {r2:.4f}")
    print("=" * 50)

    # Feature Importance Ranking
    importances = pd.Series(rf_model.feature_importances_, index=features).sort_values(ascending=False)
    print("\nTop Feature Importances (Physical Fundamentals vs Lags):")
    for feat, imp in importances.items():
        print(f"  {feat:<22}: {imp * 100:.2f}%")

    return rf_model, y_test, predictions


def plot_forecast_results(y_test, predictions):
    """
    Plots actual vs predicted electricity prices to visualize model performance.
    """
    print("\nGenerating forecast comparison chart...")

    # Calculate metrics for title
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    # Set up the plot size
    plt.figure(figsize=(13, 6))

    # Plot actual prices (Blue line)
    plt.plot(
        y_test.values,
        label='Actual Prices (EUR/MWh)',
        color='#1f77b4',
        linewidth=2,
        marker='o',
        markersize=4
    )

    # Plot predicted prices (Red dashed line)
    plt.plot(
        predictions,
        label='Predicted Prices (EUR/MWh)',
        color='#d62728',
        linestyle='--',
        linewidth=2,
        marker='x',
        markersize=5
    )

    # Customize the chart appearance
    plt.title(
        f'Day-Ahead Electricity Price Forecasting: Actual vs Predicted\n'
        f'(R² Score = {r2:.4f} | MAE = {mae:.2f} EUR/MWh)',
        fontsize=13,
        pad=12,
        fontweight='bold'
    )
    plt.xlabel('Test Set Timeline (Hours)', fontsize=11)
    plt.ylabel('Price (EUR/MWh)', fontsize=11)

    plt.legend(fontsize=11, loc='upper right')
    plt.grid(True, linestyle=':', alpha=0.7)

    plt.tight_layout()

    # Save the plot as an image file in the project folder
    image_filename = 'forecast_comparison.png'
    plt.savefig(image_filename, dpi=300, bbox_inches='tight')
    print(f"Chart successfully saved as '{image_filename}'")

    # Display the plot
    try:
        plt.show(block=False)
        plt.pause(1)
    except Exception:
        pass
    plt.close()


# -- Entry Point ---------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Random Forest - Day-Ahead Price Forecasting")
    print("  (Enhanced with SMARD Physical Fundamentals)")
    print("=" * 60)

    # Fetch multi-week dataset with physical market fundamentals
    print("\n[1/3] Fetching 4 weeks of price & fundamental data from SMARD API...")
    df = fetch_smard_fundamentals(weeks=4)

    # Fallback to single-week price data if fundamentals unavailable
    if df is None:
        print("[WARN] Falling back to standard price data...")
        df = fetch_smard_data()

    if df is None:
        print("Failed to fetch data from SMARD API. Exiting.")
        exit(1)

    # Train model with fundamentals
    print("\n[2/3] Training model...")
    rf_model, actual_prices, predicted_prices = train_price_forecasting_model(df)

    # Plot forecast comparison
    print("\n[3/3] Plotting forecast results...")
    plot_forecast_results(actual_prices, predicted_prices)

    print("\n" + "=" * 60)
    print("  Forecasting complete.")
    print("=" * 60)
