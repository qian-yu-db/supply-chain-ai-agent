"""
Supply Chain Control Tower - Chat with the Shipment
Streamlit app that connects to the Supply Chain Disruption Agent (MAS endpoint).
"""

import json
import os

import requests
import streamlit as st
from databricks.sdk.core import Config

st.set_page_config(
    page_title="Supply Chain Control Tower",
    page_icon="🏭",  # noqa: RUF001
    layout="wide",
)

# --- Auth ---
cfg = Config()
SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT_NAME", "mas-a682127d-endpoint")


def get_headers():
    headers = {"Content-Type": "application/json"}
    headers.update(cfg.authenticate())
    return headers


def query_agent(messages: list[dict]) -> str:
    """Send messages to the MAS agent endpoint and extract the response."""
    url = f"{cfg.host}/serving-endpoints/{SERVING_ENDPOINT}/invocations"
    payload = {"input": messages}

    resp = requests.post(url, headers=get_headers(), json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    # Extract text from the MAS output format
    output = data.get("output", [])
    texts = []
    for item in output:
        if item.get("type") == "message" and item.get("role") == "assistant":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    text = content.get("text", "")
                    # Skip internal routing tags
                    if text.startswith("<name>") and text.endswith("</name>"):
                        continue
                    if text:
                        texts.append(text)

    return "\n\n".join(texts) if texts else "No response from agent."


# --- Sidebar ---
with st.sidebar:
    st.title("Supply Chain Control Tower")
    st.markdown("**Chat with the Shipment**")
    st.markdown("---")
    st.markdown("**Sample Questions:**")
    sample_questions = [
        "Which production lines are at CRITICAL risk?",
        "Show me all shipments in a storm zone",
        "What is the total financial exposure?",
        "Which suppliers have critical inventory levels?",
        "List delayed purchase orders and affected lines",
        "What is the cost impact if PL-003 shuts down?",
    ]
    for q in sample_questions:
        if st.button(q, key=q, use_container_width=True):
            st.session_state["pending_question"] = q

    st.markdown("---")
    st.markdown(
        "**Agent:** `mas-a682127d-endpoint`\n\n"
        "**Data:** `tko_mtv_goup5.supply_chain_qyu`"
    )
    if st.button("Clear Chat", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()

# --- Chat ---
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# Display chat history
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Handle input (from text box or sidebar button)
pending = st.session_state.pop("pending_question", None)
user_input = st.chat_input("Ask about shipments, suppliers, or disruptions...")
prompt = pending or user_input

if prompt:
    # Add user message
    st.session_state["messages"].append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Query agent
    with st.chat_message("assistant"):
        with st.spinner("Analyzing supply chain data..."):
            agent_messages = [
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state["messages"]
            ]
            response = query_agent(agent_messages)
        st.markdown(response)

    st.session_state["messages"].append({"role": "assistant", "content": response})
