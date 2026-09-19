"""
rf_model.py - Random Forest Model for Day-Ahead Electricity Price Forecasting

Trains a Random Forest Regressor using time-based and lag features
derived from SMARD API data to predict German electricity prices.
"""

import pandas as pd
import matplotlib.pyplot as plt
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


def plot_forecast_results(y_test, predictions):
    """
    Plots the actual vs predicted electricity prices to visualize model performance.
    """
    print("Generating forecast comparison chart...")
    
    # Set up the plot size
    plt.figure(figsize=(12, 6))
    
    # Plot actual prices (Blue line)
    # Resetting index so the x-axis just shows chronological hours (0 to 23 for a day)
    plt.plot(y_test.values, label='Actual Prices (EUR/MWh)', color='#1f77b4', linewidth=2, marker='o')
    
    # Plot predicted prices (Red dashed line)
    plt.plot(predictions, label='Predicted Prices (EUR/MWh)', color='#d62728', linestyle='--', linewidth=2, marker='x')
    
    # Customize the chart appearance
    plt.title('Day-Ahead Electricity Price Forecasting: Actual vs Predicted', fontsize=14, pad=15)
    plt.xlabel('Test Set Timeline (Hours)', fontsize=12)
    plt.ylabel('Price (EUR/MWh)', fontsize=12)
    
    plt.legend(fontsize=11)
    plt.grid(True, linestyle=':', alpha=0.7)
    
    # Add a tight layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot as an image file in the project folder
    image_filename = 'forecast_comparison.png'
    plt.savefig(image_filename, dpi=300, bbox_inches='tight')
    print(f"Chart successfully saved as '{image_filename}'")
    
    # Display the plot on the screen
    plt.show()


# -- Entry Point ---------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  Random Forest - Day-Ahead Price Forecasting")
    print("=" * 60)

    # Fetch data from SMARD API
    print("\n[1/3] Fetching data from SMARD API...")
    df = fetch_smard_data()

    if df is None:
        print("Failed to fetch data. Exiting.")
        exit(1)

    # Train the model
    print("\n[2/3] Training model...")
    model, actual_prices, predicted_prices = train_price_forecasting_model(df)

    # Plot forecast results
    print("\n[3/3] Plotting forecast results...")
    plot_forecast_results(actual_prices, predicted_prices)

    print("\n" + "=" * 60)
    print("  Forecasting complete.")
    print("=" * 60)
