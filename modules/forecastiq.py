# =====================================
# CENTRALIQ - ForecastIQ Module
# Purpose: Forecast CPI using 3 models
# ARIMA + XGBoost + Ensemble
# This module = Research Paper 1
# =====================================

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import MinMaxScaler
import xgboost as xgb
import warnings
warnings.filterwarnings('ignore')

# ── DATABASE CONNECTION ───────────────
def get_engine():
    return create_engine(
        "postgresql://postgres:Karan@localhost/centraliq"
    )

# ── LOAD CPI DATA ─────────────────────
def load_cpi_data():
    """Load clean CPI data from CoreVault"""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT date, value
        FROM macro_indicators
        WHERE indicator_name = 'CPI'
        AND source = 'RBI'
        ORDER BY date ASC
    """, engine)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df.columns = ['CPI']
    print(f"✅ Loaded {len(df)} months of CPI data")
    print(f"   Range: {df.index[0].date()} → {df.index[-1].date()}")
    return df

# ── TRAIN/TEST SPLIT ──────────────────
def split_data(df, test_months=12):
    """
    Split data into training and testing sets
    
    test_months: how many months to use for testing
                 We test on last 12 months
                 Train on everything before that
    """
    train = df.iloc[:-test_months]
    test = df.iloc[-test_months:]
    print(f"\n📊 Data Split:")
    print(f"   Train: {len(train)} months ({train.index[0].date()} → {train.index[-1].date()})")
    print(f"   Test:  {len(test)} months  ({test.index[0].date()} → {test.index[-1].date()})")
    return train, test

# ── MODEL 1: ARIMA ────────────────────
def run_arima(train, test):
    """
    ARIMA = AutoRegressive Integrated Moving Average
    
    The classic statistical forecasting model.
    Uses past values to predict future values.
    This is our BASELINE model.
    """
    print("\n" + "="*50)
    print("📈 MODEL 1: ARIMA (Baseline)")
    print("="*50)
    
    try:
        from pmdarima import auto_arima
        
        print("   Finding best ARIMA parameters...")
        
        # auto_arima automatically finds best p,d,q values
        model = auto_arima(
            train['CPI'],
            seasonal=True,
            m=12,              # monthly seasonality
            stepwise=True,
            suppress_warnings=True,
            error_action='ignore'
        )
        
        print(f"   Best model: ARIMA{model.order}")
        
        # Forecast for test period
        forecast = model.predict(n_periods=len(test))
        forecast_series = pd.Series(
            forecast,
            index=test.index
        )
        
        # Calculate errors
        mae = mean_absolute_error(test['CPI'], forecast_series)
        rmse = np.sqrt(mean_squared_error(test['CPI'], forecast_series))
        mape = np.mean(np.abs((test['CPI'] - forecast_series) / test['CPI'])) * 100
        
        print(f"\n   📊 ARIMA Results:")
        print(f"   MAE  (Mean Absolute Error):      {mae:.4f}")
        print(f"   RMSE (Root Mean Square Error):   {rmse:.4f}")
        print(f"   MAPE (Mean Absolute % Error):    {mape:.2f}%")
        
        print(f"\n   📋 Forecast vs Actual (last 6 months):")
        comparison = pd.DataFrame({
            'Actual': test['CPI'].tail(6),
            'ARIMA_Forecast': forecast_series.tail(6)
        })
        print(comparison.to_string())
        
        return {
            'model': 'ARIMA',
            'mae': mae,
            'rmse': rmse,
            'mape': mape,
            'forecast': forecast_series
        }
        
    except Exception as e:
        print(f"   ❌ ARIMA Error: {e}")
        return None

# ── CREATE LAG FEATURES ───────────────
def create_features(df, n_lags=12):
    """
    Create lag features for ML models
    
    Lag features = past values used as inputs
    
    Example:
    To predict March CPI, we use:
    lag_1 = February CPI
    lag_2 = January CPI
    lag_3 = December CPI
    ...up to 12 months back
    
    Also add:
    month number (1-12) for seasonality
    year for trend
    """
    df_features = df.copy()
    
    # Add lag features
    for i in range(1, n_lags + 1):
        df_features[f'lag_{i}'] = df_features['CPI'].shift(i)
    
    # Add time features
    df_features['month'] = df_features.index.month
    df_features['year'] = df_features.index.year
    
    # Add rolling averages
    df_features['rolling_3'] = df_features['CPI'].shift(1).rolling(3).mean()
    df_features['rolling_6'] = df_features['CPI'].shift(1).rolling(6).mean()
    df_features['rolling_12'] = df_features['CPI'].shift(1).rolling(12).mean()
    
    # Drop rows with NaN (from lag creation)
    df_features = df_features.dropna()
    
    return df_features

# ── MODEL 2: XGBOOST ──────────────────
def run_xgboost(train, test):
    """
    XGBoost = Extreme Gradient Boosting
    
    A powerful ML algorithm that builds many
    small decision trees and combines them.
    Much more flexible than ARIMA.
    Can use multiple features (lag values, trends etc.)
    """
    print("\n" + "="*50)
    print("🤖 MODEL 2: XGBoost (Machine Learning)")
    print("="*50)
    
    try:
        # Combine train and test for feature creation
        full_df = pd.concat([train, test])
        featured_df = create_features(full_df)
        
        # Split back into train/test
        train_featured = featured_df.iloc[:-len(test)]
        test_featured = featured_df.iloc[-len(test):]
        
        # Separate features (X) from target (y)
        feature_cols = [c for c in featured_df.columns if c != 'CPI']
        X_train = train_featured[feature_cols]
        y_train = train_featured['CPI']
        X_test = test_featured[feature_cols]
        y_test = test_featured['CPI']
        
        # Train XGBoost model
        model = xgb.XGBRegressor(
            n_estimators=200,
            learning_rate=0.05,
            max_depth=4,
            subsample=0.8,
            random_state=42,
            verbosity=0
        )
        model.fit(X_train, y_train)
        
        # Predict
        predictions = model.predict(X_test)
        forecast_series = pd.Series(predictions, index=y_test.index)
        
        # Calculate errors
        mae = mean_absolute_error(y_test, predictions)
        rmse = np.sqrt(mean_squared_error(y_test, predictions))
        mape = np.mean(np.abs((y_test - predictions) / y_test)) * 100
        
        print(f"\n   📊 XGBoost Results:")
        print(f"   MAE  (Mean Absolute Error):      {mae:.4f}")
        print(f"   RMSE (Root Mean Square Error):   {rmse:.4f}")
        print(f"   MAPE (Mean Absolute % Error):    {mape:.2f}%")
        
        # Feature importance
        importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\n   🔍 Top 5 Most Important Features:")
        print(importance.head(5).to_string(index=False))
        
        print(f"\n   📋 Forecast vs Actual (last 6 months):")
        comparison = pd.DataFrame({
            'Actual': y_test.tail(6),
            'XGBoost_Forecast': forecast_series.tail(6)
        })
        print(comparison.to_string())
        
        return {
            'model': 'XGBoost',
            'mae': mae,
            'rmse': rmse,
            'mape': mape,
            'forecast': forecast_series
        }
        
    except Exception as e:
        print(f"   ❌ XGBoost Error: {e}")
        return None

# ── MODEL 3: ENSEMBLE ─────────────────
def run_ensemble(arima_result, xgb_result, test):
    """
    Ensemble = Combine multiple models
    
    Instead of trusting just one model,
    we take a weighted average of predictions.
    
    ARIMA weight: 40% (good at trends)
    XGBoost weight: 60% (good at patterns)
    
    Combined → usually beats individual models
    """
    print("\n" + "="*50)
    print("🎯 MODEL 3: Ensemble (ARIMA + XGBoost)")
    print("="*50)
    
    if arima_result is None or xgb_result is None:
        print("   ❌ Need both models to create ensemble")
        return None
    
    try:
        # Get common index
        arima_fc = arima_result['forecast']
        xgb_fc = xgb_result['forecast']
        
        # Align indices
        common_idx = arima_fc.index.intersection(xgb_fc.index)
        arima_fc = arima_fc[common_idx]
        xgb_fc = xgb_fc[common_idx]
        actual = test['CPI'][common_idx]
        
        # Weighted average
        weights = {'arima': 0.4, 'xgboost': 0.6}
        ensemble_fc = (
            weights['arima'] * arima_fc +
            weights['xgboost'] * xgb_fc
        )
        
        # Calculate errors
        mae = mean_absolute_error(actual, ensemble_fc)
        rmse = np.sqrt(mean_squared_error(actual, ensemble_fc))
        mape = np.mean(np.abs((actual - ensemble_fc) / actual)) * 100
        
        print(f"   Weights: ARIMA={weights['arima']*100}%  XGBoost={weights['xgboost']*100}%")
        print(f"\n   📊 Ensemble Results:")
        print(f"   MAE  (Mean Absolute Error):      {mae:.4f}")
        print(f"   RMSE (Root Mean Square Error):   {rmse:.4f}")
        print(f"   MAPE (Mean Absolute % Error):    {mape:.2f}%")
        
        print(f"\n   📋 Forecast vs Actual (last 6 months):")
        comparison = pd.DataFrame({
            'Actual': actual.tail(6),
            'Ensemble_Forecast': ensemble_fc.tail(6)
        })
        print(comparison.to_string())
        
        return {
            'model': 'Ensemble',
            'mae': mae,
            'rmse': rmse,
            'mape': mape,
            'forecast': ensemble_fc
        }
        
    except Exception as e:
        print(f"   ❌ Ensemble Error: {e}")
        return None

# ── COMPARE ALL MODELS ────────────────
def compare_models(results):
    """
    Create final comparison table
    This table = core of your research paper
    """
    print("\n" + "="*50)
    print("🏆 FINAL MODEL COMPARISON")
    print("="*50)
    print("(This is your Research Paper Table!)\n")
    
    rows = []
    for r in results:
        if r is not None:
            rows.append({
                'Model': r['model'],
                'MAE': round(r['mae'], 4),
                'RMSE': round(r['rmse'], 4),
                'MAPE (%)': round(r['mape'], 2)
            })
    
    comparison_df = pd.DataFrame(rows)
    print(comparison_df.to_string(index=False))
    
    # Find best model
    best = comparison_df.loc[comparison_df['RMSE'].idxmin()]
    print(f"\n🥇 Best Model: {best['Model']}")
    print(f"   RMSE: {best['RMSE']} | MAE: {best['MAE']} | MAPE: {best['MAPE (%)']}%")
    
    return comparison_df

# ── SAVE RESULTS TO DATABASE ──────────
def save_forecast_results(results):
    """Save forecast results to CoreVault"""
    engine = get_engine()
    
    for result in results:
        if result is None:
            continue
        
        rows = []
        for date, value in result['forecast'].items():
            rows.append({
                'indicator_name': f"FORECAST_CPI_{result['model'].upper()}",
                'value': float(value),
                'date': date,
                'source': 'ForecastIQ',
                'sector': 'Forecast'
            })
        
        if rows:
            df = pd.DataFrame(rows)
            df.to_sql(
                'macro_indicators',
                engine,
                if_exists='append',
                index=False
            )
            print(f"✅ Saved {result['model']} forecasts to CoreVault")

# ── RUN FORECASTIQ ────────────────────
def run_forecastiq():
    print("="*50)
    print("🔮 CENTRALIQ ForecastIQ Starting...")
    print("="*50)
    
    # Load data
    df = load_cpi_data()
    
    # Split into train/test
    train, test = split_data(df, test_months=12)
    
    # Run all 3 models
    arima_result = run_arima(train, test)
    xgb_result = run_xgboost(train, test)
    ensemble_result = run_ensemble(arima_result, xgb_result, test)
    
    # Compare results
    all_results = [arima_result, xgb_result, ensemble_result]
    comparison = compare_models([r for r in all_results if r])
    
    # Save forecasts to database
    print("\n💾 Saving forecasts to CoreVault...")
    save_forecast_results([r for r in all_results if r])
    
    print("\n✅ ForecastIQ Complete!")
    print("🎓 Your Research Paper Table is ready above!")

# ── START ─────────────────────────────
run_forecastiq()