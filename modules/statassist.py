# =====================================
# CENTRALIQ - StatAssist Module
# Purpose: RAG-powered Statistical
# Assistant using RBI Documents
# Maps to DSIM: Technical Support
# =====================================

import os
from dotenv import load_dotenv
import requests
import json

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# ── PDF READER ────────────────────────
def read_pdf(filepath):
    """Extract text from PDF file"""
    print(f"📄 Reading PDF: {filepath}")
    try:
        import pypdf
        reader = pypdf.PdfReader(filepath)
        text = ""
        total_pages = len(reader.pages)
        print(f"   Total pages: {total_pages}")
        
        # Read first 50 pages (enough for context)
        pages_to_read = min(50, total_pages)
        for i in range(pages_to_read):
            page_text = reader.pages[i].extract_text()
            if page_text:
                text += f"\n--- Page {i+1} ---\n{page_text}"
        
        print(f"   ✅ Read {pages_to_read} pages")
        print(f"   Total text: {len(text)} characters")
        return text
    except Exception as e:
        print(f"   ❌ Error reading PDF: {e}")
        return None

# ── TEXT CHUNKER ──────────────────────
def chunk_text(text, chunk_size=2000, overlap=200):
    """
    Split large text into smaller chunks
    
    Why chunks?
    LLMs have limited context window
    We can't feed entire PDF at once
    So we split into chunks and find
    the most relevant ones per question
    
    overlap: chunks share some text
    so context isn't lost at boundaries
    """
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append({
            'text': chunk,
            'start': start,
            'end': end
        })
        start = end - overlap
    
    print(f"   ✅ Created {len(chunks)} text chunks")
    return chunks

# ── SIMPLE KEYWORD SEARCH ─────────────
def find_relevant_chunks(query, chunks, top_k=5):
    """
    Find most relevant chunks for a query
    Uses simple keyword matching
    (Simple but effective for our purpose)
    """
    query_words = set(query.lower().split())
    
    scored_chunks = []
    for i, chunk in enumerate(chunks):
        chunk_words = set(chunk['text'].lower().split())
        # Score = number of query words found in chunk
        score = len(query_words.intersection(chunk_words))
        scored_chunks.append((score, i, chunk))
    
    # Sort by score, highest first
    scored_chunks.sort(reverse=True, key=lambda x: x[0])
    
    # Return top k chunks
    top_chunks = [c[2] for c in scored_chunks[:top_k]]
    return top_chunks

# ── CALL GROQ AI ──────────────────────
def call_groq(system_prompt, user_message, max_tokens=800):
    """Call Groq AI with system + user message"""
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    body = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.3
    }
    
    try:
        response = requests.post(
            GROQ_URL,
            headers=headers,
            json=body,
            timeout=30
        )
        data = response.json()
        if 'choices' not in data:
            print(f"❌ API Error: {data}")
            return None
        return data['choices'][0]['message']['content']
    except Exception as e:
        print(f"❌ Groq error: {e}")
        return None

# ── ANSWER QUESTION ───────────────────
def answer_question(query, chunks, doc_name="RBI Annual Report 2024"):
    """
    Answer a question using relevant document chunks
    This is the RAG process:
    R = Retrieve relevant chunks
    A = Augment prompt with chunks
    G = Generate answer using AI
    """
    print(f"\n🔍 Finding relevant sections...")
    
    # R - Retrieve
    relevant = find_relevant_chunks(query, chunks, top_k=5)
    
    if not relevant:
        return "No relevant information found."
    
    # A - Augment
    context = "\n\n".join([c['text'] for c in relevant])
    
    system_prompt = f"""You are a statistical analyst at the Reserve Bank of India.
You answer questions based ONLY on the provided document excerpts.
Document: {doc_name}
Always cite specific data points and figures when available.
If information is not in the context, say "This information is not available in the provided document."
Keep answers concise and professional."""
    
    user_message = f"""Context from {doc_name}:
{context[:3000]}

Question: {query}

Please answer based on the context above."""
    
    # G - Generate
    print(f"🤖 Generating answer...")
    answer = call_groq(system_prompt, user_message)
    
    return answer

# ── INTERACTIVE CHAT ──────────────────
def run_chat(chunks, doc_name):
    """Run interactive Q&A session"""
    print("\n" + "="*55)
    print("💬 StatAssist — RBI Statistical Query System")
    print("="*55)
    print(f"📚 Knowledge Base: {doc_name}")
    print("Type your question and press Enter")
    print("Type 'exit' to quit")
    print("="*55)
    
    # Sample questions to show capability
    sample_questions = [
        "What is India's GDP growth rate?",
        "What is the inflation target of RBI?",
        "What are the key monetary policy decisions?",
        "How is the banking sector performing?"
    ]
    
    print("\n💡 Sample questions you can ask:")
    for i, q in enumerate(sample_questions, 1):
        print(f"   {i}. {q}")
    
    # Auto-answer sample questions first
    print("\n" + "="*55)
    print("📋 AUTO-DEMO: Answering sample questions...")
    print("="*55)
    
    for question in sample_questions[:2]:
        print(f"\n❓ Question: {question}")
        print("-"*55)
        answer = answer_question(question, chunks, doc_name)
        if answer:
            print(f"📊 Answer:\n{answer}")
        print("-"*55)
    
    # Interactive mode
    print("\n" + "="*55)
    print("🎯 Now ask YOUR questions!")
    print("="*55)
    
    while True:
        print()
        user_input = input("❓ Your question (or 'exit'): ").strip()
        
        if user_input.lower() in ['exit', 'quit', 'q']:
            print("👋 StatAssist session ended.")
            break
        
        if not user_input:
            continue
        
        answer = answer_question(user_input, chunks, doc_name)
        if answer:
            print(f"\n📊 Answer:\n{answer}")

# ── RUN STATASSIST ────────────────────
def run_statassist():
    print("="*55)
    print("🤖 CENTRALIQ StatAssist Starting...")
    print("="*55)
    
    # Load RBI document
    pdf_path = r'C:\Users\jangi\Desktop\CENTRALIQ\data\rbi_docs\rbi_annual_2024.pdf'
    
    # Read PDF
    text = read_pdf(pdf_path)
    
    if text is None:
        print("❌ Could not read PDF. Check file path.")
        return
    
    # Create chunks
    print(f"\n✂️ Splitting document into chunks...")
    chunks = chunk_text(text)
    
    # Run chat
    run_chat(chunks, "RBI Annual Report 2024")

# ── START ─────────────────────────────
run_statassist()