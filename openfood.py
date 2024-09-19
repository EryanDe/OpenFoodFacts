import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import psycopg2  # Pour la connexion à PostgreSQL
from datetime import date

# Configuration du SparkSession
spark = SparkSession.builder \
    .appName("OpenFoodFactsETL") \
    .getOrCreate()

# Définition des régimes alimentaires
REGIMES = {
    'vegetarien': {
        'exclude_categories': ['viande', 'poisson', 'oeuf'],
        'caloric_limit': 2000,  # Exemple de limite calorique
        'sodium_limit': 2,      # Exemple de limite pour le sodium
        'fat_limit': 70,        # Exemple de limite pour les lipides
        'protein_min': 50       # Exemple de minimum pour les protéines
    },
    'cetogene': {
        'caloric_limit': 2500,
        'sodium_limit': 3,
        'fat_limit': 150,
        'carb_limit': 20,      # Limitation stricte des glucides
        'protein_min': 60
    }
}

def load_products_from_csv(file_path):
    """
    Charge le fichier CSV OpenFoodFacts en un DataFrame Spark.
    """
    try:
        df = spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            sep='\t',
            nullValue='',
            escape='"',
            multiLine=True
        )
        print("Fichier chargé avec succès.")
        return df
    except Exception as e:
        print(f"Erreur lors du chargement du fichier CSV: {e}")
        return None

def filter_by_diet(df, diet):
    """
    Filtrer les produits en fonction du régime alimentaire.
    """
    if diet in REGIMES:
        diet_info = REGIMES[diet]
        
        # Exclusion des catégories de produits pour certains régimes
        if 'exclude_categories' in diet_info:
            for category in diet_info['exclude_categories']:
                df = df.filter(~col('categories_tags').like(f"%{category}%"))
        
        # Limites nutritionnelles spécifiques au régime
        if 'caloric_limit' in diet_info:
            df = df.filter(col('energy_100g') <= diet_info['caloric_limit'])
        if 'sodium_limit' in diet_info:
            df = df.filter(col('sodium_100g') <= diet_info['sodium_limit'])
        if 'fat_limit' in diet_info:
            df = df.filter(col('fat_100g') <= diet_info['fat_limit'])
        if 'protein_min' in diet_info:
            df = df.filter(col('proteins_100g') >= diet_info['protein_min'])
        if 'carb_limit' in diet_info:
            df = df.filter(col('sugars_100g') <= diet_info['carb_limit'])
        
        print(f"Nombre de produits après filtrage pour le régime {diet}: {df.count()}")
    else:
        print(f"Régime {diet} non défini.")
    
    return df

def insert_product_into_db(conn, product):
    """
    Insère un produit dans la table 'products' et renvoie son product_id.
    """
    try:
        cur = conn.cursor()
        cur.execute('''INSERT INTO products (product_name, calories_100g, sodium_100g, sugars_100g, fat_100g, fiber_100g, proteins_100g, nova_group)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (product_name) DO UPDATE SET calories_100g = EXCLUDED.calories_100g
                       RETURNING product_id''',
                    (product['product_name'], product['energy_100g'], product['sodium_100g'], product['sugars_100g'], 
                     product['fat_100g'], product['fiber_100g'], product['proteins_100g'], product['nova_group']))
        conn.commit()
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Erreur lors de l'insertion du produit dans la base de données : {e}")
        return None

def insert_menu(conn, user_id, total_calories):
    """
    Insère un menu hebdomadaire dans la table 'menus' et renvoie le menu_id.
    """
    try:
        cur = conn.cursor()
        week_start = date.today()
        cur.execute('''INSERT INTO menus (user_id, week_start_date, total_calories)
                       VALUES (%s, %s, %s) RETURNING menu_id''', (user_id, week_start, total_calories))
        conn.commit()
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Erreur lors de l'insertion du menu dans la base de données : {e}")
        return None

def insert_user(conn, user_info):
    """
    Insère un utilisateur dans la table 'users' et renvoie son user_id.
    """
    try:
        cur = conn.cursor()
        cur.execute('''INSERT INTO users (first_name, last_name, age, gender, weight_kg, height_cm, activity_level, diet_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING user_id''',
                    (user_info['first_name'], user_info['last_name'], user_info['age'], user_info['gender'],
                     user_info['weight_kg'], user_info['height_cm'], user_info['activity_level'], user_info['diet_id']))
        conn.commit()
        return cur.fetchone()[0]
    except Exception as e:
        print(f"Erreur lors de l'insertion de l'utilisateur dans la base de données : {e}")
        return None

def insert_daily_meals(conn, menu_id, product_id, day, meal_type, meal_info):
    """
    Insère les repas quotidiens dans la table 'daily_meals' en lien avec le menu_id et le product_id.
    """
    try:
        cur = conn.cursor()
        cur.execute('''INSERT INTO daily_meals (menu_id, product_id, day_of_week, meal_type, calories, sugars_100g, fat_100g, proteins_100g, fiber_100g)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                    (menu_id, product_id, day, meal_type, meal_info['energy_100g'], 
                     meal_info['sugars_100g'], meal_info['fat_100g'], meal_info['proteins_100g'], meal_info['fiber_100g']))
        conn.commit()
        cur.close()
        print("Repas journalier inséré dans la base de données.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des repas journaliers dans la base de données : {e}")

def generate_weekly_menu(df, conn, caloric_limit=2000):
    """
    Génère un menu hebdomadaire équilibré en respectant une limite calorique quotidienne.
    """
    menu = {}
    total_calories = 0
    days = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    meals = ["Petit Déjeuner", "Déjeuner", "Dîner"]
    
    for day in days:
        daily_menu = []
        remaining_calories = caloric_limit
        for meal in meals:
            possible_products = df.filter(col('energy_100g') <= remaining_calories)
            if possible_products.count() == 0:
                print(f"Impossible de trouver un produit pour {meal} le {day} avec les calories restantes.")
                product = None
            else:
                product = possible_products.orderBy(rand()).first()
                try:
                    energy_value = float(product['energy_100g'])
                except (ValueError, TypeError):
                    energy_value = 0
                
                remaining_calories -= energy_value
                total_calories += energy_value

                product_id = insert_product_into_db(conn, product)
                if product_id:
                    daily_menu.append({
                        'meal': meal,
                        'product_id': product_id,
                        'product_name': product['product_name'],
                        'energy_100g': product['energy_100g'],
                        'sugars_100g': product['sugars_100g'],
                        'fat_100g': product['fat_100g'],
                        'proteins_100g': product['proteins_100g'],
                        'fiber_100g': product['fiber_100g'],
                    })
        menu[day] = daily_menu
    return menu, total_calories

def insert_diets(conn):
    """
    Insère les régimes alimentaires dans la table 'diets'.
    """
    try:
        cur = conn.cursor()
        diets = [
            ('vegetarien', 2000, 70, 50, 20, 30, 10),
            ('cetogene', 2500, 150, 60, 20, 50, 15)
        ]
        for diet in diets:
            cur.execute('''INSERT INTO diets (diet_name, max_calories_per_day, max_fat_per_day, max_proteins_per_day, max_carbs_per_day, max_sugars_per_day, fiber_min_per_day)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING''', diet)
        conn.commit()
        cur.close()
        print("Régimes alimentaires insérés dans la base de données.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des régimes alimentaires : {e}")

def main():
    try:
        conn = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",
            database="openfoodfact",
            port="5432"
        )
    except psycopg2.Error as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
        return
    
    # Insère les régimes alimentaires avant d'insérer les utilisateurs
    insert_diets(conn)

    file_path = "data.csv"

    df = load_products_from_csv(file_path)
    if df is None:
        return
    
    # Exemple d'utilisateurs avec différents régimes
    users = [
        {'first_name': 'Jean', 'last_name': 'Dupont', 'age': 30, 'gender': 'Male', 'weight_kg': 75, 'height_cm': 180, 'activity_level': 'Moderate', 'diet_id': 1},
        {'first_name': 'Marie', 'last_name': 'Lemoine', 'age': 25, 'gender': 'Female', 'weight_kg': 60, 'height_cm': 165, 'activity_level': 'High', 'diet_id': 2},
        # Ajoute d'autres utilisateurs ici
    ]
    
    for user in users:
        user_id = insert_user(conn, user)
        if user_id is None:
            print(f"Erreur lors de l'insertion de l'utilisateur {user['first_name']} {user['last_name']}")
            continue

        # Filtrer par régime alimentaire
        diet_name = 'vegetarien' if user['diet_id'] == 1 else 'cetogene'
        df_clean = filter_by_diet(df, diet_name)
        if df_clean.count() == 0:
            print(f"Aucun produit disponible après filtrage pour le régime {diet_name}.")
            continue

        # Génération du menu hebdomadaire
        menu, total_calories = generate_weekly_menu(df_clean, conn, caloric_limit=2000)

        menu_id = insert_menu(conn, user_id, total_calories)
        if menu_id is None:
            print("Erreur lors de l'insertion du menu.")
            continue

        for day, meals in menu.items():
            for meal in meals:
                insert_daily_meals(conn, menu_id, meal['product_id'], day, meal['meal'], meal)

    conn.close()

if __name__ == "__main__":
    main()