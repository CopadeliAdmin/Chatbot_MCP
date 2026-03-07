import streamlit as st
import os
from back import OpenAIChat

# Configuration de la page
st.set_page_config(page_title="Copadeli - Assistant Ventes", layout="centered")

st.title("🥩 Copadeli - Analyseur de Ventes")
st.write("Posez vos questions sur la base de données fournisseur_viande.db")

# Barre latérale pour la clé API
with st.sidebar:    
    st.info("""
    Cet assistant peut répondre à des questions comme :
    - Quels sont mes meilleurs clients ?
    - Quelle est la marge moyenne par produit ?
    - Donnez-moi l'évolution mensuelle du chiffre d'affaires.
    """)

# Initialisation du chat
if "chat_engine" not in st.session_state:
    st.session_state.chat_engine = OpenAIChat()
    st.session_state.messages = []

# Affichage de l'historique des messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Entrée de l'utilisateur
if prompt := st.chat_input("Que voulez-vous savoir sur vos ventes ?"):
    # Ajouter le message de l'utilisateur à l'historique
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Réponse de l'assistant
    with st.chat_message("assistant"):
        with st.spinner("Analyse de la base de données..."):
            response = st.session_state.chat_engine.ask(prompt)
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})
