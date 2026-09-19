"""
rf_model.py - Random Forest Model for Day-Ahead Electricity Price Forecasting

Trains a Random Forest Regressor using time-based and lag features
derived from SMARD API data to predict German electricity prices.
"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

from market_data import fetch_smard_data


def train_price_forecasting_model(df):
    """
    Trains a Random Forest model to forecast day-ahead electricity prices.
    Assumes df has 'Datetime' and 'Price_EUR_MWh' columns.
    """
    print("Preparing data for the Random Forest model...")

    # Feature Engineering: Extracting time-based features
    df['Hour'] = df['Datetime'].dt.hour
    df['DayOfWeek'] = df['Datetime'].dt.dayofweek
    df['Month'] = df['Datetime'].dt.month

    # Creating lag features (previous hours' prices) to capture trends
    df['Price_Lag_1'] = df['Price_EUR_MWh'].shift(1)
    df['Price_Lag_24'] = df['Price_EUR_MWh'].shift(24)  # Same hour from the previous day

    # Drop rows with NaN values caused by shifting
    df = df.dropna()

    # Define features (X) and target variable (y)
    features = ['Hour', 'DayOfWeek', 'Month', 'Price_Lag_1', 'Price_Lag_24']
    X = df[features]
    y = df['Price_EUR_MWh']

    # Split the dataset into training (80%) and testing (20%) sets
    # shuffle=False is crucial for time-series data to prevent data leakage
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )

    print(f"Training Data: {len(X_train)} samples")
    print(f"Testing Data: {len(X_test)} samples")

    # Initialize and train the Random Forest Regressor
    print("\nTraining the model (this might take a moment)...")
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf_model.fit(X_train, y_train)

    # Make predictions on the test set
    predictions = rf_model.predict(X_test)

    # Evaluate model performance
    mae = mean_absolute_error(y_test, predictions)
    mse = mean_squared_error(y_test, predictions)

    print("\nModel Evaluation Metrics:")
    print(f"Mean Absolute Error (MAE): {mae:.2f} EUR/MWh")
    print(f"Mean Squared Error (MSE): {mse:.2f}")

    return rf_model, y_test, predictions


# -- Entry Point ---------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Random Forest - Day-Ahead Price Forecasting")
    print("=" * 60)

    # Fetch data from SMARD API
    print("\n[1/2] Fetching data from SMARD API...")
    df = fetch_smard_data()

    if df is None:
        print("Failed to fetch data. Exiting.")
        exit(1)

    # Train the model
    print("\n[2/2] Training model...")
    model, actual_prices, predicted_prices = train_price_forecasting_model(df)

    print("\n" + "=" * 60)
    print("  Forecasting complete.")
    print("=" * 60)
