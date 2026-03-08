"""
====================================================================
  CAPODELI - Générateur de base de données commerciales réaliste
  Fournisseur de viande HoReCa Thaïlande
====================================================================
Caractéristiques :
  - 10 clients (5 hôtels, 5 restaurants)
  - 120 mois de données (Jan 2020 → Déc 2024)
  - 2 factures/client/mois → ~480 factures, ~1920 lignes
  - CA cible ~100 000 €/mois (2,4 M€ sur 2 ans)
  - Saisonnalité thaïlandaise : +30% (Nov-Fév), -15% (Juin-Août)
  - Simulation d'un client qui "décroche" à partir de M+14
  - Anomalies aléatoires : pics, baisses brutales, commandes manquées
====================================================================
"""

import sqlite3
import random
import math
from datetime import date, timedelta

# ── Reproductibilité ──────────────────────────────────────────────
random.seed(42)

# ── Paramètres globaux ────────────────────────────────────────────
DB_PATH = "data/revops_demo.db"
START_DATE = date(2020, 1, 1)
END_DATE   = date(2024, 12, 31)

# ── Clients ───────────────────────────────────────────────────────
HOTELS = [
    {"name": "Dusit Thani",      "type": "Hôtel 5*",   "weight": 1.20},
    {"name": "Intercontinental", "type": "Hôtel 5*",   "weight": 1.15},
    {"name": "Holiday Inn",      "type": "Hôtel 4*",   "weight": 1.10},
    {"name": "Amari",            "type": "Hôtel 4*",   "weight": 1.05},
    {"name": "CenterPoint",      "type": "Hôtel 3*",   "weight": 0.90},
]
RESTAURANTS = [
    {"name": "Chez Papa",    "type": "Restaurant Français", "weight": 1.10},
    {"name": "La Cagette",   "type": "Restaurant de plage", "weight": 0.90},
    {"name": "El Mercado",   "type": "Restaurant Espagnol", "weight": 1.00},
    {"name": "Arnaud",       "type": "Restaurant Gastronomique", "weight": 1.15},
    {"name": "Scrlette",     "type": "Restaurant de plage", "weight": 0.85},
]
ALL_CLIENTS = HOTELS + RESTAURANTS

# Client qui "décroche" — commence à baisser à partir du mois 14
CHURN_CLIENT = "La Cagette"
CHURN_START_MONTH = 14   # mois 14/24 (février 2024)

# ── Produits ──────────────────────────────────────────────────────
PRODUCTS = {
    "Bœuf premium": {
        "ca_share": 0.40,
        "price_base": 22.0,
        "cost_base":  14.0,
        "price_noise": 1.5,   # ±€/kg
    },
    "Porc": {
        "ca_share": 0.20,
        "price_base": 12.0,
        "cost_base":   7.0,
        "price_noise": 0.8,
    },
    "Poulet": {
        "ca_share": 0.20,
        "price_base":  9.0,
        "cost_base":   5.0,
        "price_noise": 0.6,
    },
    "Charcuterie": {
        "ca_share": 0.20,
        "price_base": 18.0,
        "cost_base":  11.0,
        "price_noise": 1.2,
    },
}

# ── Poids produits par type client ────────────────────────────────
# Les hôtels commandent plus de poulet (buffet/breakfast),
# les restaurants premium plus de bœuf.
PRODUCT_WEIGHTS_BY_TYPE = {
    "Hôtel 5*":                   {"Bœuf premium": 0.35, "Porc": 0.20, "Poulet": 0.30, "Charcuterie": 0.15},
    "Hôtel 4*":                   {"Bœuf premium": 0.38, "Porc": 0.22, "Poulet": 0.25, "Charcuterie": 0.15},
    "Hôtel 3*":                   {"Bœuf premium": 0.30, "Porc": 0.25, "Poulet": 0.30, "Charcuterie": 0.15},
    "Restaurant Gastronomique":   {"Bœuf premium": 0.60, "Porc": 0.10, "Poulet": 0.10, "Charcuterie": 0.20},
    "Restaurant Français":        {"Bœuf premium": 0.50, "Porc": 0.20, "Poulet": 0.10, "Charcuterie": 0.20},
    "Restaurant Espagnol":        {"Bœuf premium": 0.40, "Porc": 0.25, "Poulet": 0.15, "Charcuterie": 0.20},
    "Restaurant de plage":        {"Bœuf premium": 0.30, "Porc": 0.20, "Poulet": 0.30, "Charcuterie": 0.20},
}

# ── Saisonnalité ──────────────────────────────────────────────────
SEASONALITY = {
    1: 1.30,   # Janvier   — Haute saison
    2: 1.30,   # Février   — Haute saison
    3: 1.10,   # Mars      — Post-saison
    4: 1.00,   # Avril     — Normale
    5: 0.95,   # Mai       — Légère baisse
    6: 0.85,   # Juin      — Basse saison
    7: 0.85,   # Juillet   — Basse saison
    8: 0.85,   # Août      — Basse saison
    9: 0.95,   # Septembre — Remontée
    10: 1.05,  # Octobre   — Pré-saison
    11: 1.30,  # Novembre  — Haute saison
    12: 1.30,  # Décembre  — Haute saison / Noël
}

# ── Cible CA mensuelle ────────────────────────────────────────────
# Hôtels 65%, Restaurants 35%
MONTHLY_CA_TARGET = 100_000  # €
HOTEL_SHARE       = 0.65
RESTAURANT_SHARE  = 0.35

# ── Utilitaires ───────────────────────────────────────────────────

def iter_months(start: date, end: date):
    """Génère (année, mois, numéro_ordinal) pour chaque mois dans la plage."""
    current = start.replace(day=1)
    ordinal = 0
    while current <= end.replace(day=1):
        ordinal += 1
        yield current.year, current.month, ordinal
        # Avance d'un mois
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def random_day_in_month(year: int, month: int) -> date:
    """Renvoie un jour ouvré aléatoire dans le mois (évite dimanche)."""
    import calendar
    _, last_day = calendar.monthrange(year, month)
    d = random.randint(1, last_day)
    chosen = date(year, month, d)
    # Si dimanche → veille
    if chosen.weekday() == 6:
        chosen -= timedelta(days=1)
    return chosen


def churn_factor(client_name: str, month_ordinal: int) -> float:
    """
    Simule le décrochage progressif du client CHURN_CLIENT.
    Facteur : 1.0 → décroissance progressive → ~0.15 au mois 24
    """
    if client_name != CHURN_CLIENT or month_ordinal < CHURN_START_MONTH:
        return 1.0
    months_since_churn = month_ordinal - CHURN_START_MONTH
    total_decay_months = 24 - CHURN_START_MONTH  # 10 mois
    # Décroissance exponentielle + bruit
    decay = math.exp(-0.20 * months_since_churn)
    noise = random.uniform(0.90, 1.10)
    # Peut aussi "sauter" une commande après mois 18
    if month_ordinal >= 20 and random.random() < 0.40:
        return 0.0  # commande annulée
    return max(0.10, decay * noise)


def anomaly_factor() -> float:
    """
    Aléatoire global : pic exceptionnel ou baisse brutale (~5% des factures).
    """
    r = random.random()
    if r < 0.02:
        return random.uniform(1.8, 2.5)   # pic exceptionnel
    elif r < 0.05:
        return random.uniform(0.3, 0.55)  # baisse brutale
    else:
        return random.uniform(0.85, 1.15) # bruit normal


def generate_invoice_lines(
    client: dict,
    year: int,
    month: int,
    month_ordinal: int,
    invoice_number: str,
    invoice_date: date,
    invoice_target_ca: float,
) -> list[dict]:
    """
    Génère 1 à 4 lignes produit pour une facture donnée.
    Retourne une liste de dicts prêts pour l'insertion SQL.
    """
    client_type = client["type"]
    prod_weights = PRODUCT_WEIGHTS_BY_TYPE.get(client_type, {p: 0.25 for p in PRODUCTS})

    # Choisir 2-4 produits distincts pour cette facture
    nb_products = random.randint(2, 4)
    products = random.choices(
        list(prod_weights.keys()),
        weights=list(prod_weights.values()),
        k=nb_products,
    )
    # Déduplique tout en conservant l'ordre
    seen = set()
    products = [p for p in products if not (p in seen or seen.add(p))]

    lines = []
    remaining_ca = invoice_target_ca

    for i, product_name in enumerate(products):
        prod = PRODUCTS[product_name]
        is_last = (i == len(products) - 1)

        # Répartition du CA entre produits selon leur part
        weight_sum = sum(prod_weights[p] for p in products)
        share = prod_weights[product_name] / weight_sum

        if is_last:
            line_ca = remaining_ca
        else:
            line_ca = invoice_target_ca * share * random.uniform(0.80, 1.20)
            remaining_ca -= line_ca
            remaining_ca = max(remaining_ca, 1.0)

        # Prix unitaire avec bruit
        unit_price = round(
            prod["price_base"] + random.uniform(-prod["price_noise"], prod["price_noise"]),
            2,
        )
        unit_price = max(unit_price, prod["cost_base"] + 0.50)

        # Quantité déduite du CA et du prix
        quantity_kg = max(0.5, round(line_ca / unit_price, 2))
        revenue    = round(quantity_kg * unit_price, 2)

        # Coût avec légère variation
        unit_cost = round(
            prod["cost_base"] * random.uniform(0.92, 1.08),
            2,
        )
        cost   = round(quantity_kg * unit_cost, 2)
        margin = round(revenue - cost, 2)
        margin_pct = round((margin / revenue) * 100, 2) if revenue > 0 else 0.0

        due_date = invoice_date + timedelta(days=random.choice([30, 45, 60]))

        lines.append({
            "Client":         client["name"],
            "ClientType":     client_type,
            "InvoiceNumber":  invoice_number,
            "InvoiceDate":    str(invoice_date),
            "DueDate":        str(due_date),
            "Product":        product_name,
            "Quantity_kg":    quantity_kg,
            "UnitPrice_EUR":  unit_price,
            "Revenue_EUR":    revenue,
            "Cost_EUR":       cost,
            "Margin_EUR":     margin,
            "Margin_pct":     margin_pct,
        })

    return lines


# ── Génération principale ─────────────────────────────────────────

def generate_database():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # Supprime et recrée la table
    cur.execute("DROP TABLE IF EXISTS commandes")
    cur.execute("""
        CREATE TABLE commandes (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            Client         TEXT,
            ClientType     TEXT,
            InvoiceNumber  TEXT,
            InvoiceDate    TEXT,
            DueDate        TEXT,
            Product        TEXT,
            Quantity_kg    REAL,
            UnitPrice_EUR  REAL,
            Revenue_EUR    REAL,
            Cost_EUR       REAL,
            Margin_EUR     REAL,
            Margin_pct     REAL
        )
    """)

    all_rows = []
    invoice_counter = 1

    for year, month, ordinal in iter_months(START_DATE, END_DATE):
        season_mult = SEASONALITY[month]

        # CA mensuel cible ajusté par la saisonnalité
        monthly_ca = MONTHLY_CA_TARGET * season_mult

        # Répartition hôtels / restaurants
        hotel_ca      = monthly_ca * HOTEL_SHARE
        restaurant_ca = monthly_ca * RESTAURANT_SHARE

        # CA par client (pondéré par weight)
        hotel_total_weight      = sum(c["weight"] for c in HOTELS)
        restaurant_total_weight = sum(c["weight"] for c in RESTAURANTS)

        for client in ALL_CLIENTS:
            is_hotel = client in HOTELS
            group_ca = hotel_ca if is_hotel else restaurant_ca
            group_weight = hotel_total_weight if is_hotel else restaurant_total_weight

            # CA mensuel de ce client
            client_monthly_ca = (group_ca * client["weight"] / group_weight)

            # Facteur décrochage
            cf = churn_factor(client["name"], ordinal)
            if cf == 0.0:
                # Le client annule toutes ses commandes ce mois-ci
                continue

            client_monthly_ca *= cf

            # 2 factures par mois, on les sépare dans le temps
            for invoice_idx in range(2):
                # Anomalie aléatoire par facture
                af = anomaly_factor()

                invoice_ca = (client_monthly_ca / 2) * af
                invoice_ca = max(invoice_ca, 10.0)

                inv_date = random_day_in_month(year, month)
                # La 2e facture est ~10-18 jours après la 1re
                if invoice_idx == 1:
                    inv_date += timedelta(days=random.randint(10, 18))
                    # Garantit qu'on reste dans le mois
                    import calendar
                    _, last = calendar.monthrange(year, month)
                    if inv_date.day > last or inv_date.month != month:
                        inv_date = date(year, month, last)

                inv_number = f"CAP-{year}-{invoice_counter:04d}"
                invoice_counter += 1

                lines = generate_invoice_lines(
                    client, year, month, ordinal,
                    inv_number, inv_date, invoice_ca,
                )
                all_rows.extend(lines)

    # Insertion en batch
    cur.executemany("""
        INSERT INTO commandes
            (Client, ClientType, InvoiceNumber, InvoiceDate, DueDate,
             Product, Quantity_kg, UnitPrice_EUR, Revenue_EUR,
             Cost_EUR, Margin_EUR, Margin_pct)
        VALUES
            (:Client, :ClientType, :InvoiceNumber, :InvoiceDate, :DueDate,
             :Product, :Quantity_kg, :UnitPrice_EUR, :Revenue_EUR,
             :Cost_EUR, :Margin_EUR, :Margin_pct)
    """, all_rows)

    conn.commit()

    # ── Rapport de contrôle ──────────────────────────────────────
    print("=" * 60)
    print("  CAPODELI — Base de données générée avec succès !")
    print("=" * 60)

    total_rows = cur.execute("SELECT COUNT(*) FROM commandes").fetchone()[0]
    total_inv  = cur.execute("SELECT COUNT(DISTINCT InvoiceNumber) FROM commandes").fetchone()[0]
    total_ca   = cur.execute("SELECT ROUND(SUM(Revenue_EUR),2) FROM commandes").fetchone()[0]
    total_margin = cur.execute("SELECT ROUND(AVG(Margin_pct),2) FROM commandes").fetchone()[0]

    print(f"  Lignes produit   : {total_rows:>8,}")
    print(f"  Factures         : {total_inv:>8,}")
    print(f"  CA total         : {total_ca:>10,.2f} €")
    print(f"  Marge moyenne    : {total_margin:>8.2f} %")
    print()

    print("  CA par client (décroissant) :")
    rows = cur.execute("""
        SELECT Client, ClientType,
               ROUND(SUM(Revenue_EUR),0) AS ca,
               ROUND(AVG(Margin_pct),2)  AS marge
        FROM commandes
        GROUP BY Client
        ORDER BY ca DESC
    """).fetchall()
    for r in rows:
        churn_tag = " ⚠️  DÉCROCHAGE" if r[0] == CHURN_CLIENT else ""
        print(f"    {r[0]:<22} | {r[2]:>10,.0f} € | marge {r[3]}%{churn_tag}")

    print()
    print("  Évolution mensuelle (La Cagette — client décrochant) :")
    rows_churn = cur.execute("""
        SELECT strftime('%Y-%m', InvoiceDate) AS mois,
               ROUND(SUM(Revenue_EUR),0) AS ca
        FROM commandes
        WHERE Client = ?
        GROUP BY mois
        ORDER BY mois
    """, (CHURN_CLIENT,)).fetchall()
    for r in rows_churn:
        bar = "█" * int(r[1] / 500)
        print(f"    {r[0]}  {r[1]:>7,.0f} €  {bar}")

    conn.close()
    print()
    print(f"  ✅  Fichier : {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    generate_database()