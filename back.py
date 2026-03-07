import os
import sqlite3
import re
import json
from openai import OpenAI

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


class OpenAIChat:
    def __init__(self):
        self.client = OpenAI()
        # On utilise le modèle gpt-4 comme indiqué dans l'exemple
        self.model = "gpt-4"
        self.input_list = []
        self.tools = [
            {
                "type": "function",
                "name": "query_database",
                "description": (
                    "Exécute une requête SELECT sur la base de données SQLite 'fournisseur_viande.db'. "
                    "La table disponible est 'commandes' avec les colonnes: "
                    "Client, ClientType, InvoiceNumber, InvoiceDate, DueDate, Product, Quantity_kg, UnitPrice_EUR, Revenue_EUR, Cost_EUR, Margin_EUR, Margin_pct. "
                    "Utilisez cet outil pour répondre aux questions sur les ventes, les clients, les produits et les marges."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql_query": {
                            "type": "string",
                            "description": "La requête SQL SELECT à exécuter.",
                        },
                    },
                    "required": ["sql_query"],
                },
            },
        ]

    def ask(self, prompt: str):
        try:
            # Ajouter le message de l'utilisateur à l'historique
            self.input_list.append({"role": "user", "content": prompt})

            # Premier appel au modèle avec les outils définis
            response = self.client.responses.create(
                model=self.model,
                tools=self.tools,
                input=self.input_list,
            )

            # Enregistrer les sorties (incluant les appels de fonctions potentiels)
            self.input_list += response.output

            tool_called = False
            for item in response.output:
                if item.type == "function_call":
                    if item.name == "query_database":
                        tool_called = True
                        # Exécuter la logique de la fonction pour query_database
                        arguments = json.loads(item.arguments)
                        result = query_database(arguments.get("sql_query", ""))

                        # Fournir les résultats de l'appel de fonction au modèle
                        self.input_list.append({
                            "type": "function_call_output",
                            "call_id": item.call_id,
                            "output": json.dumps({
                                "result": result
                            })
                        })

            # Si un outil a été appelé, on sollicite à nouveau le modèle pour obtenir la réponse finale
            if tool_called:
                response = self.client.responses.create(
                    model=self.model,
                    tools=self.tools,
                    input=self.input_list,
                )
                # Enregistrer la réponse finale dans l'historique
                self.input_list += response.output

            return response.output_text
        except Exception as e:
            return f"Désolé, une erreur est survenue : {str(e)}"


if __name__ == "__main__":
    # Test simple (nécessite OPENAI_API_KEY en variable d'environnement)
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        chat = OpenAIChat()
        print(
            chat.ask(
                "Quels sont les 3 meilleurs clients en termes de chiffre d'affaires ?"
            )
        )
    else:
        print("Veuillez configurer OPENAI_API_KEY pour tester back.py")
