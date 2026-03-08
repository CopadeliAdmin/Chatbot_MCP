import streamlit as st
import os
from back import OpenAIChat

# Configuration de la page
st.set_page_config(
    page_title="Copadeli Business Intelligence",
    page_icon=":material/analytics:",
    layout="centered"
)

# Style CSS personnalisé pour masquer le branding et l'animation d'état
st.markdown("""
    <style>
        /* Masquer le menu Streamlit, le pied de page et l'en-tête */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Masquer le widget de statut "Running..." (animation "sport") */
        div[data-testid="stStatusWidget"] {display: none;}
        
        /* Ajustement de la typographie pour un look plus pro */
        h1, h2, h3 {
            font-family: 'Inter', sans-serif;
            font-weight: 700;
        }
    </style>
    """, unsafe_allow_html=True)

# Logo de l'entreprise
st.logo("https://www.copadeli.com/wp-content/uploads/2021/05/logo-copadeli.png", icon_image=":material/analytics:")

# En-tête de l'application
st.title("Business intelligence")
st.subheader("Assistant d'analyse de ventes", divider="blue")
st.caption("Interrogez vos données commerciales en langage naturel via l'IA Copadeli.")

# Barre latérale pour le contexte et les suggestions
with st.sidebar:
    st.markdown("### :material/lightbulb: Suggestions")
    with st.container(border=True):
        st.markdown("""
        - **Meilleurs clients** : Quels sont mes 5 meilleurs clients par chiffre d'affaires ?
        - **Produits** : Quelle est la marge moyenne par produit ?
        - **Tendances** : Donnez-moi l'évolution mensuelle des ventes.
        """)
    
    st.divider()
    st.caption("Powered by Copadeli AI Engine v1.2")

# Initialisation du chat
if "chat_engine" not in st.session_state:
    st.session_state.chat_engine = OpenAIChat()
    st.session_state.messages = []
    # Assurez-vous que la clé est disponible dans secrets.toml
    if "OPENAI_API_KEY" in st.secrets:
        os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]
    else:
        st.error("Clé API OpenAI manquante dans les secrets.")

# Affichage de l'historique des messages
for message in st.session_state.messages:
    with st.chat_message(message["role"],avatar=":material/robot:" if message["role"] == "assistant" else ":material/person:"):
        st.markdown(message["content"])

# Entrée de l'utilisateur
if prompt := st.chat_input("Que voulez-vous savoir sur vos ventes ?"):
    # Ajouter le message de l'utilisateur à l'historique
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user",avatar=":material/person:"):
        st.markdown(prompt)

    # Réponse de l'assistant
    with st.chat_message("assistant", avatar=":material/smart_toy:"):
        # Utilisation de st.status pour une animation de chargement discrète
        with st.status(":material/database: Analyse des données en cours...", expanded=False) as status:
            response_generator = st.session_state.chat_engine.ask(prompt)
            try:
                # On avance le générateur pour effectuer l'appel API et les outils SQL
                first_chunk = next(response_generator)
            except StopIteration:
                first_chunk = ""
            status.update(label=":material/check_circle: Analyse terminée", state="complete", expanded=False)
        
        # On affiche la réponse en streaming en dehors du bloc status
        def stream_output():
            yield first_chunk
            yield from response_generator
            
        response = st.write_stream(stream_output())
        st.session_state.messages.append({"role": "assistant", "content": response})

