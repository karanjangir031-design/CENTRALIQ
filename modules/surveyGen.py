# =====================================
# CENTRALIQ - SurveyGen Module
# Purpose: AI-powered Survey Designer
# Maps to DSIM: Sample Survey Planning
# =====================================

import requests
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
import random

# ── CONFIG ────────────────────────────
GROQ_API_KEY = "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"  
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# ── DATABASE ──────────────────────────
def get_engine():
    return create_engine(
        "postgresql://postgres:Karan@localhost/centraliq"
    )

# ── CALL GROQ AI ──────────────────────
def call_groq(prompt, max_tokens=1000):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    body = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }
    
    try:
        response = requests.post(GROQ_URL, headers=headers, json=body, timeout=30)
        data = response.json()
        
        # Print full response for debugging
        if 'choices' not in data:
            print(f"   ❌ Unexpected response: {data}")
            return None
            
        return data['choices'][0]['message']['content']
    except Exception as e:
        print(f"   ❌ Groq API error: {e}")
        return None

# ── GENERATE SURVEY ───────────────────
def generate_survey(topic, target_audience, num_questions=8):
    """
    Use AI to generate a professional survey
    Similar to RBI's Inflation Expectations Survey
    """
    print(f"\n🎯 Generating survey on: {topic}")
    print(f"   Target: {target_audience}")
    
    prompt = f"""
    You are an RBI economist designing an official survey.
    
    Topic: {topic}
    Target Audience: {target_audience}
    Number of Questions: {num_questions}
    
    Create a professional survey with exactly {num_questions} questions.
    Mix these question types:
    - Likert scale (1-5 rating)
    - Multiple choice
    - Yes/No
    
    Return ONLY a JSON array like this:
    [
        {{
            "id": 1,
            "question": "question text here",
            "type": "likert",
            "options": ["Strongly Disagree", "Disagree", "Neutral", "Agree", "Strongly Agree"]
        }},
        {{
            "id": 2,
            "question": "question text here", 
            "type": "mcq",
            "options": ["Option A", "Option B", "Option C", "Option D"]
        }},
        {{
            "id": 3,
            "question": "question text here",
            "type": "yesno",
            "options": ["Yes", "No"]
        }}
    ]
    
    Return ONLY the JSON array. No other text.
    """
    
    response = call_groq(prompt)
    
    if response is None:
        return None
    
    try:
        # Clean response and parse JSON
        response = response.strip()
        if response.startswith("```"):
            response = response.split("```")[1]
            if response.startswith("json"):
                response = response[4:]
        
        questions = json.loads(response)
        print(f"   ✅ Generated {len(questions)} questions")
        return questions
        
    except Exception as e:
        print(f"   ❌ Could not parse survey: {e}")
        return None

# ── SIMULATE RESPONSES ────────────────
def simulate_responses(questions, num_respondents=100):
    """
    Simulate survey responses
    In real DSIM usage, real people would fill this
    For now we simulate to show analysis pipeline
    """
    print(f"\n👥 Simulating {num_respondents} respondents...")
    
    all_responses = []
    
    for i in range(num_respondents):
        response = {'respondent_id': i + 1}
        
        for q in questions:
            options = q.get('options', [])
            if not options:
                continue
            
            # Simulate realistic response patterns
            if q['type'] == 'likert':
                # Slightly positive bias (normal distribution)
                weights = [0.1, 0.2, 0.3, 0.25, 0.15]
                response[f"q{q['id']}"] = random.choices(options, weights=weights)[0]
                
            elif q['type'] == 'yesno':
                response[f"q{q['id']}"] = random.choices(options, weights=[0.6, 0.4])[0]
                
            else:  # mcq
                response[f"q{q['id']}"] = random.choice(options)
        
        all_responses.append(response)
    
    df = pd.DataFrame(all_responses)
    print(f"   ✅ {len(df)} responses collected")
    return df

# ── ANALYZE RESPONSES ─────────────────
def analyze_responses(questions, responses_df):
    """
    Statistical analysis of survey responses
    Similar to how RBI analyzes survey data
    """
    print(f"\n📊 Analyzing survey responses...")
    print("="*55)
    
    analysis_results = []
    
    for q in questions:
        q_col = f"q{q['id']}"
        
        if q_col not in responses_df.columns:
            continue
        
        print(f"\nQ{q['id']}: {q['question'][:60]}...")
        
        # Frequency count
        counts = responses_df[q_col].value_counts()
        percentages = (counts / len(responses_df) * 100).round(1)
        
        for option, count in counts.items():
            pct = percentages[option]
            bar = "█" * int(pct / 5)
            print(f"   {option[:25]:<25} {bar} {pct}%")
        
        # For Likert scales — calculate mean sentiment
        if q['type'] == 'likert':
            likert_map = {
                'Strongly Disagree': 1,
                'Disagree': 2,
                'Neutral': 3,
                'Agree': 4,
                'Strongly Agree': 5
            }
            numeric = responses_df[q_col].map(likert_map)
            mean_score = numeric.mean()
            
            if mean_score >= 4:
                sentiment = "Positive 📈"
            elif mean_score >= 3:
                sentiment = "Neutral ➡️"
            else:
                sentiment = "Negative 📉"
            
            print(f"   Mean Score: {mean_score:.2f}/5 → {sentiment}")
        
        analysis_results.append({
            'question_id': q['id'],
            'question': q['question'],
            'type': q['type'],
            'top_response': counts.index[0],
            'top_pct': percentages.iloc[0]
        })
    
    return pd.DataFrame(analysis_results)

# ── GENERATE AI SUMMARY ───────────────
def generate_ai_summary(topic, analysis_df):
    """
    Use AI to write a professional summary
    of survey findings — like an RBI report
    """
    print(f"\n📝 Generating AI Summary Report...")
    
    # Prepare findings for AI
    findings = []
    for _, row in analysis_df.iterrows():
        findings.append(
            f"Q{row['question_id']}: {row['question'][:50]} → "
            f"Top response: '{row['top_response']}' ({row['top_pct']}%)"
        )
    
    findings_text = "\n".join(findings)
    
    prompt = f"""
    You are an RBI economist writing a survey report.
    
    Survey Topic: {topic}
    
    Key Findings:
    {findings_text}
    
    Write a professional 3-paragraph summary report of these findings.
    Use formal language appropriate for a central bank publication.
    Include policy implications in the last paragraph.
    Keep it under 200 words.
    """
    
    summary = call_groq(prompt, max_tokens=400)
    
    if summary:
        print("\n" + "="*55)
        print("📋 OFFICIAL SURVEY SUMMARY REPORT")
        print("="*55)
        print(summary)
        print("="*55)
    
    return summary

# ── SAVE SURVEY TO DATABASE ───────────
def save_survey_results(topic, analysis_df):
    """Save survey results to CoreVault"""
    engine = get_engine()
    
    rows = []
    for _, row in analysis_df.iterrows():
        rows.append({
            'indicator_name': f"SURVEY_{topic.upper().replace(' ', '_')[:20]}",
            'value': float(row['top_pct']),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'SurveyGen',
            'sector': 'Survey'
        })
    
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df.to_sql('macro_indicators', engine, if_exists='append', index=False)
    print(f"\n✅ Survey results saved to CoreVault")

# ── RUN SURVEYGEN ─────────────────────
def run_surveygen():
    print("="*55)
    print("📋 CENTRALIQ SurveyGen Starting...")
    print("="*55)
    
    # Survey 1: Inflation Expectations
    # (mirrors RBI's actual survey)
    topic1 = "Inflation Expectations"
    audience1 = "Indian households and small businesses"
    
    questions1 = generate_survey(topic1, audience1, num_questions=6)
    
    if questions1:
        # Show questions
        print(f"\n📋 Generated Survey Questions:")
        for q in questions1:
            print(f"   Q{q['id']}: {q['question']}")
            print(f"        Type: {q['type']}")
        
        # Simulate and analyze
        responses1 = simulate_responses(questions1, num_respondents=200)
        analysis1 = analyze_responses(questions1, responses1)
        
        # AI summary
        generate_ai_summary(topic1, analysis1)
        
        # Save results
        save_survey_results(topic1, analysis1)
    
    print(f"\n✅ SurveyGen Complete!")

# ── START ─────────────────────────────
run_surveygen()