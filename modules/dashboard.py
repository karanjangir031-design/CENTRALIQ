# =====================================
# CENTRALIQ - Dashboard Module
# Purpose: Visual Interface for all
# CENTRALIQ modules
# =====================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import json

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# ── PAGE CONFIG ───────────────────────
st.set_page_config(
    page_title="CENTRALIQ",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── DATABASE ──────────────────────────
def get_engine():
    return create_engine(
        "postgresql://postgres:Karan@localhost/centraliq"
    )

def load_data(indicator_name):
    engine = get_engine()
    df = pd.read_sql(f"""
        SELECT date, value
        FROM macro_indicators
        WHERE indicator_name = '{indicator_name}'
        ORDER BY date ASC
    """, engine)
    df['date'] = pd.to_datetime(df['date'])
    return df

def load_summary():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT indicator_name, COUNT(*) as records,
               MIN(date) as earliest, MAX(date) as latest,
               ROUND(AVG(value)::numeric, 2) as avg_value,
               source
        FROM macro_indicators
        GROUP BY indicator_name, source
        ORDER BY source, indicator_name
    """, engine)
    return df

# ── GROQ AI ───────────────────────────
def ask_ai(question, context=""):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    system = """You are a senior RBI economist and statistical analyst.
    Answer questions about Indian macroeconomics professionally.
    Use data provided in context when available.
    Keep answers concise and cite specific figures."""
    
    body = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}
        ],
        "max_tokens": 500
    }
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers, json=body, timeout=30
        )
        return r.json()['choices'][0]['message']['content']
    except:
        return "AI service unavailable. Please try again."

# ── CUSTOM CSS ────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1a1a2e, #16213e, #0f3460);
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    .metric-card {
        background: #16213e;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #e94560;
    }
    .stMetric {
        background: #16213e;
        padding: 10px;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# ── SIDEBAR ───────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Reserve_Bank_of_India_seal.svg/200px-Reserve_Bank_of_India_seal.svg.png", width=80)
    st.title("CENTRALIQ")
    st.caption("AI-Powered Macroeconomic\nIntelligence System")
    st.divider()
    
    page = st.selectbox(
        "📊 Select Module",
        [
            "🏠 Overview",
            "📈 CPI Analysis",
            "🔮 Forecasts",
            "📡 Market Signals",
            "🤖 Data Quality",
            "💬 StatAssist AI",
            "📋 Survey Results"
        ]
    )
    
    st.divider()
    st.caption("DSIM — Reserve Bank of India")
    st.caption(f"Last updated: {datetime.now().strftime('%d %b %Y')}")

# ════════════════════════════════════
# PAGE 1: OVERVIEW
# ════════════════════════════════════
if page == "🏠 Overview":
    st.markdown("""
    <div class='main-header'>
        <h1 style='color:white; margin:0'>🏦 CENTRALIQ</h1>
        <p style='color:#aaa; margin:0'>AI-Powered Macroeconomic Intelligence System for Central Banks</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    # Load latest CPI
    cpi_df = load_data('CPI')
    latest_cpi = cpi_df['value'].iloc[-1]
    prev_cpi = cpi_df['value'].iloc[-2]
    cpi_change = latest_cpi - prev_cpi
    
    # Load GDP growth
    gdp_df = load_data('GDP_GROWTH')
    latest_gdp = gdp_df['value'].iloc[-1]
    
    # Load inflation rate
    inf_df = load_data('INFLATION_RATE')
    latest_inf = inf_df['value'].iloc[-1]
    
    # Load unemployment
    unemp_df = load_data('UNEMPLOYMENT')
    latest_unemp = unemp_df['value'].iloc[-1]
    
    with col1:
        st.metric("🏷️ CPI Index", f"{latest_cpi:.1f}",
                 f"{cpi_change:+.1f} vs prev month")
    with col2:
        st.metric("📈 GDP Growth", f"{latest_gdp:.2f}%",
                 "Latest annual")
    with col3:
        st.metric("💰 Inflation Rate", f"{latest_inf:.2f}%",
                 "Latest annual")
    with col4:
        st.metric("👷 Unemployment", f"{latest_unemp:.2f}%",
                 "Latest annual")
    
    st.divider()
    
    # Quick CPI chart
    st.subheader("📈 India CPI — Historical Trend")
    fig = px.line(
        cpi_df,
        x='date', y='value',
        title='Consumer Price Index (Base: 2012=100)',
        color_discrete_sequence=['#e94560']
    )
    fig.update_layout(
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white',
        xaxis_title="Date",
        yaxis_title="CPI Value"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Database summary
    st.subheader("📦 CoreVault — Data Summary")
    summary = load_summary()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Indicators", len(summary))
    with col2:
        st.metric("Total Records", summary['records'].sum())
    with col3:
        st.metric("Data Sources", summary['source'].nunique())
    
    st.dataframe(
        summary[['indicator_name', 'records', 'earliest', 'latest', 'source']],
        use_container_width=True,
        hide_index=True
    )

# ════════════════════════════════════
# PAGE 2: CPI ANALYSIS
# ════════════════════════════════════
elif page == "📈 CPI Analysis":
    st.title("📈 CPI Deep Analysis")
    st.caption("Consumer Price Index — India (Base: 2012=100)")
    
    df = load_data('CPI')
    
    # Date filter
    col1, col2 = st.columns(2)
    with col1:
        start_year = st.slider("Start Year", 2013, 2025, 2018)
    with col2:
        end_year = st.slider("End Year", 2013, 2025, 2025)
    
    filtered = df[
        (df['date'].dt.year >= start_year) &
        (df['date'].dt.year <= end_year)
    ]
    
    # Main chart
    fig = px.line(
        filtered, x='date', y='value',
        title=f'CPI Index {start_year}–{end_year}',
        color_discrete_sequence=['#00d4ff']
    )
    fig.update_layout(
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # YoY change
    df['yoy_change'] = df['value'].pct_change(12) * 100
    df_yoy = df.dropna()
    
    fig2 = px.bar(
        df_yoy[df_yoy['date'].dt.year >= start_year],
        x='date', y='yoy_change',
        title='Year-on-Year CPI Change (%)',
        color='yoy_change',
        color_continuous_scale='RdYlGn_r'
    )
    fig2.update_layout(
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white'
    )
    st.plotly_chart(fig2, use_container_width=True)
    
    # Stats
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Min CPI", f"{filtered['value'].min():.1f}")
    with col2:
        st.metric("Max CPI", f"{filtered['value'].max():.1f}")
    with col3:
        st.metric("Average", f"{filtered['value'].mean():.1f}")
    with col4:
        st.metric("Latest", f"{filtered['value'].iloc[-1]:.1f}")

# ════════════════════════════════════
# PAGE 3: FORECASTS
# ════════════════════════════════════
elif page == "🔮 Forecasts":
    st.title("🔮 ForecastIQ — CPI Predictions")
    st.caption("ML Model Comparison: ARIMA vs XGBoost vs Ensemble")
    
    # Load actual and forecasts
    actual = load_data('CPI')
    arima = load_data('FORECAST_CPI_ARIMA')
    xgb = load_data('FORECAST_CPI_XGBOOST')
    ensemble = load_data('FORECAST_CPI_ENSEMBLE')
    
    # Combined chart
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=actual['date'].tail(24), y=actual['value'].tail(24),
        name='Actual CPI', line=dict(color='#00d4ff', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=arima['date'], y=arima['value'],
        name='ARIMA', line=dict(color='#ff6b6b', dash='dash')
    ))
    fig.add_trace(go.Scatter(
        x=xgb['date'], y=xgb['value'],
        name='XGBoost', line=dict(color='#51cf66', dash='dash')
    ))
    fig.add_trace(go.Scatter(
        x=ensemble['date'], y=ensemble['value'],
        name='Ensemble', line=dict(color='#ffd43b', dash='dot')
    ))
    
    fig.update_layout(
        title='CPI Actual vs Forecasts',
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white',
        xaxis_title='Date',
        yaxis_title='CPI Value'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Model comparison table
    st.subheader("🏆 Model Performance — Research Paper Table")
    
    results = pd.DataFrame({
        'Model': ['ARIMA', 'XGBoost', 'Ensemble'],
        'MAE': [4.8579, 1.2823, 1.8315],
        'RMSE': [4.9964, 1.5338, 1.9057],
        'MAPE (%)': [2.49, 0.66, 0.94],
        'Rank': ['🥉 3rd', '🥇 1st', '🥈 2nd']
    })
    
    st.dataframe(results, use_container_width=True, hide_index=True)
    
    st.success("🎯 Key Finding: XGBoost reduces forecast error by 73.5% vs ARIMA (MAPE: 0.66% vs 2.49%)")

# ════════════════════════════════════
# PAGE 4: MARKET SIGNALS
# ════════════════════════════════════
elif page == "📡 Market Signals":
    st.title("📡 Market Signals — Google Trends")
    st.caption("Real-time economic sentiment from search behavior")
    
    keywords = {
        'Petrol Price': 'TREND_PETROL_PRICE',
        'Home Loan': 'TREND_HOME_LOAN',
        'Inflation': 'TREND_INFLATION',
        'Job Vacancy': 'TREND_JOB_VACANCY'
    }
    
    selected = st.multiselect(
        "Select Keywords",
        list(keywords.keys()),
        default=list(keywords.keys())
    )
    
    fig = go.Figure()
    colors = ['#e94560', '#00d4ff', '#51cf66', '#ffd43b']
    
    for i, keyword in enumerate(selected):
        df = load_data(keywords[keyword])
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['value'],
            name=keyword,
            line=dict(color=colors[i % len(colors)])
        ))
    
    fig.update_layout(
        title='Google Trends — Economic Keywords (India)',
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white',
        xaxis_title='Date',
        yaxis_title='Search Interest (0-100)'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Latest values
    st.subheader("📊 Latest Trend Values")
    cols = st.columns(len(selected))
    for i, keyword in enumerate(selected):
        df = load_data(keywords[keyword])
        latest = df['value'].iloc[-1]
        prev = df['value'].iloc[-2]
        with cols[i]:
            st.metric(keyword, f"{latest:.0f}", f"{latest-prev:+.0f}")

# ════════════════════════════════════
# PAGE 5: DATA QUALITY
# ════════════════════════════════════
elif page == "🤖 Data Quality":
    st.title("🤖 MethoBot — Data Quality Report")
    st.caption("Automated data quality monitoring for CoreVault")
    
    quality_data = {
        'Indicator': ['CPI', 'GDP', 'GDP_GROWTH', 'INFLATION_RATE',
                     'UNEMPLOYMENT', 'TREND_*', 'FORECAST_*'],
        'Score': [95, 95, 85, 95, 95, 90, 80],
        'Grade': ['🟢 Excellent', '🟢 Excellent', '🟡 Good',
                 '🟢 Excellent', '🟢 Excellent', '🟢 Excellent', '🟡 Good'],
        'Issues': [1, 1, 2, 1, 1, 1, 1],
        'Source': ['RBI', 'WorldBank', 'WorldBank', 'WorldBank',
                  'WorldBank', 'GoogleTrends', 'ForecastIQ']
    }
    
    df_quality = pd.DataFrame(quality_data)
    avg_score = sum(quality_data['Score']) / len(quality_data['Score'])
    
    # Overall score
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Overall Quality Score", f"{avg_score:.1f}/100")
    with col2:
        st.metric("Indicators Monitored", len(quality_data['Indicator']))
    with col3:
        st.metric("Total Issues", sum(quality_data['Issues']))
    
    # Score chart
    fig = px.bar(
        df_quality, x='Indicator', y='Score',
        title='Data Quality Scores by Indicator',
        color='Score',
        color_continuous_scale='RdYlGn',
        range_color=[0, 100]
    )
    fig.update_layout(
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(df_quality, use_container_width=True, hide_index=True)

# ════════════════════════════════════
# PAGE 6: STATASSIST
# ════════════════════════════════════
elif page == "💬 StatAssist AI":
    st.title("💬 StatAssist — AI Statistical Assistant")
    st.caption("Ask questions about Indian macroeconomics")
    
    # Load context
    engine = get_engine()
    summary = pd.read_sql("""
        SELECT indicator_name, 
               ROUND(AVG(value)::numeric,2) as avg,
               MAX(value) as max,
               MIN(value) as min,
               MAX(date) as latest_date
        FROM macro_indicators
        GROUP BY indicator_name
    """, engine)
    context = summary.to_string()
    
    # Sample questions
    st.subheader("💡 Sample Questions")
    cols = st.columns(2)
    samples = [
        "What is India's GDP growth trend?",
        "How has CPI changed over 10 years?",
        "Compare inflation vs CPI trends",
        "What does Google Trends tell us about economy?"
    ]
    
    for i, q in enumerate(samples):
        with cols[i % 2]:
            if st.button(q, key=f"sample_{i}"):
                st.session_state['question'] = q
    
    st.divider()
    
    # Chat interface
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    # Display history
    for msg in st.session_state.messages:
        with st.chat_message(msg['role']):
            st.write(msg['content'])
    
    # Input
    question = st.chat_input("Ask about Indian macroeconomics...")
    
    if 'question' in st.session_state:
        question = st.session_state.pop('question')
    
    if question:
        st.session_state.messages.append(
            {"role": "user", "content": question}
        )
        with st.chat_message("user"):
            st.write(question)
        
        with st.chat_message("assistant"):
            with st.spinner("Analyzing data..."):
                answer = ask_ai(question, context)
            st.write(answer)
            st.session_state.messages.append(
                {"role": "assistant", "content": answer}
            )

# ════════════════════════════════════
# PAGE 7: SURVEY RESULTS
# ════════════════════════════════════
elif page == "📋 Survey Results":
    st.title("📋 SurveyGen — Survey Analytics")
    st.caption("AI-generated surveys on macroeconomic expectations")
    
    st.subheader("📊 Inflation Expectations Survey — April 2026")
    st.caption("200 simulated respondents | Indian households & businesses")
    
    # Survey results
    questions = [
        "Concerned about inflation?",
        "Income keep pace with inflation?",
        "Changed spending habits?"
    ]
    
    yes_pct = [48.5, 57.5, 61.5]
    no_pct = [51.5, 42.5, 38.5]
    
    fig = go.Figure(data=[
        go.Bar(name='Yes/Concerned', y=questions,
               x=yes_pct, orientation='h',
               marker_color='#e94560'),
        go.Bar(name='No/Not Concerned', y=questions,
               x=no_pct, orientation='h',
               marker_color='#00d4ff')
    ])
    
    fig.update_layout(
        barmode='stack',
        title='Survey Response Distribution (%)',
        plot_bgcolor='#1a1a2e',
        paper_bgcolor='#1a1a2e',
        font_color='white'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.info("""
    **Key Finding:** 61.5% of respondents have changed their 
    spending/saving habits due to inflation concerns.
    This suggests significant impact on consumption patterns — 
    relevant for RBI's monetary policy formulation.
    """)
    
    st.subheader("📝 AI-Generated Policy Summary")
    st.write("""
    The survey on inflation expectations reveals nuanced public 
    perception. Respondents show moderate concern about current 
    inflation, with mixed expectations about future price movements.
    
    A majority (57.5%) expect household income to keep pace with 
    inflation, suggesting cautious optimism. However, the finding 
    that 61.5% have adjusted spending/saving habits signals 
    potential impact on consumption and savings rates.
    
    **Policy Implication:** Policymakers should monitor these 
    behavioral changes when formulating monetary policy to 
    maintain economic stability.
    """)