import os
import sqlite3
import re
from google import genai
from google.genai import types

# Configuration
DB_PATH = os.path.join(os.path.dirname(__file__), "fournisseur_viande.db")


# Tool definition for SQL execution
def query_database(sql_query: str) -> str:
    """
    Exécute une requête SELECT sur la base de données SQLite 'fournisseur_viande.db'.
    La table disponible est 'commandes' avec les colonnes:
    Client, ClientType, InvoiceNumber, InvoiceDate, DueDate, Product, Quantity_kg, UnitPrice_EUR, Revenue_EUR, Cost_EUR, Margin_EUR, Margin_pct.
    Utilisez cet outil pour répondre aux questions sur les ventes, les clients, les produits et les marges.
    """
    # Security: Only allow SELECT
    if not re.match(r"^\s*SELECT", sql_query, re.IGNORECASE):
        return "Erreur : Seules les requêtes SELECT sont autorisées."

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        conn.close()

        if not results:
            return "Aucun résultat trouvé pour cette requête."

        return f"Colonnes: {columns}\nDonnées: {results}"
    except Exception as e:
        return f"Erreur lors de l'exécution de la requête : {str(e)}"


class GeminiChat:
    def __init__(self):
        client = genai.Client()
        self.chat = client.chats.create(
            model="gemini-2.0-flash",
            config=types.GenerateContentConfig(tools=[query_database]),
        )

    def ask(self, prompt: str):
        try:
            response = self.chat.send_message(prompt)
            return response.text
        except Exception as e:
            return f"Désolé, une erreur est survenue : {str(e)}"


if __name__ == "__main__":
    # Test simple (nécessite GEMINI_API_KEY en variable d'environnement)
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        chat = GeminiChat()
        print(
            chat.ask(
                "Quels sont les 3 meilleurs clients en termes de chiffre d'affaires ?"
            )
        )
    else:
        print("Veuillez configurer GEMINI_API_KEY pour tester back.py")
