import geopandas as gpd
import pandas as pd
from matplotlib import *
import matplotlib as plot 
import numpy as np
import random 
from math import *
import matplotlib.pyplot as plt
from datetime import * 
import pygwalker as pyg
import os
import requests
from dotenv import load_dotenv
from dateutil import parser
import streamlit as st
import plotly.express as px
import streamlit_option_menu as option_menu 
from datetime import timedelta
from pygwalker.api.streamlit import StreamlitRenderer


df_utilisateur = pd.read_csv("investors.csv")
df_operations = pd.read_csv("operations.csv")
df_products = pd.read_csv("products.csv")
df_forums = pd.read_csv("forums.csv")

df_utilisateur = df_utilisateur.where(pd.notnull(df_utilisateur), None)
df_operations = df_operations.where(pd.notnull(df_operations), None)
df_products = df_products.where(pd.notnull(df_products), None)
df_forums = df_forums.where(pd.notnull(df_forums), None)


#Création des colonnes Dates extraites et Age
def extraire_dates(colonne_dates):

    # Convertir la colonne en datetime si elle ne l'est pas déjà
    colonne_dates = pd.to_datetime(colonne_dates, errors='coerce')

    # Extraire les dates sous le bon format 
    dates_extraites = colonne_dates.dt.date
    return dates_extraites
df_utilisateur['Dates_extraites'] = extraire_dates(df_utilisateur['birth_date'])

def calculate_age(birth_date):
    today = datetime.now()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age
 
df_utilisateur['Age'] = df_utilisateur['Dates_extraites'].apply(calculate_age)

#Head of the page 
st.set_page_config(page_title="epargnoo data analyse", page_icon="epargnoo_favicon_orange_blanc.png",layout='centered', initial_sidebar_state='auto')

#Side bar
st.sidebar.image("2223_09_09-livrable_rvb-logo-bleu-orange.png",caption="online analytics")

# Options pour le menu déroulant dans la sidebar
options_sidebar = ['Analyse totale & 7 jours', 'Analyse libre']
choix_page_sidebar = st.sidebar.selectbox('Sélectionnez une page', options_sidebar)

#Df avec que les opé 1-4-5
df_operations_valide = df_operations[
    (df_operations.operation_status_id == 1) |
    (df_operations.operation_status_id == 4) |
    (df_operations.operation_status_id == 5)
]
#Merge des DF Oparation et produit
df_operat_valide_merged=df_products.merge(df_operations_valide,on="product_id")
df_operat_product_merged=df_products.merge(df_operations,on="product_id")

#Dataframe pour la carte Streamlit pour les investisseurs
df_cities=pd.read_csv("villes.csv",sep=';')
df_cities=df_cities[["Postal Code","latitude","longitude"]]
df_cities["adress_postal_code"]=df_cities["Postal Code"]

#Création de la colonne département_code
def extract_first_two_digits(df, column_name):
    df['département_code'] = df[column_name].astype(str).str[:2]
    return df
df_utilisateur=extract_first_two_digits(df_utilisateur, "adress_postal_code")

#Changement du type de la date Createdat
df_utilisateur['createdAt'] = pd.to_datetime(df_utilisateur['createdAt'], unit='ms')
aujourd_hui = datetime.today().date()
df_utilisateur['createdAt'] = df_utilisateur['createdAt'].dt.date

#Dataframe des villes avec des données manquantes 
df_cities["adress_postal_code"]=df_cities["adress_postal_code"].astype(str)
df_unique = df_cities.drop_duplicates(subset='Postal Code')

#Merge des 3 DataFrames 
df_All=df_utilisateur.merge(df_operat_valide_merged, on="investor_id")

#Merge avec le DF des villes lat / long
df_All["adress_postal_code"]=df_All["adress_postal_code"].astype(str)
df_All["company_fiscal_address_postal_code"]=df_All["company_fiscal_address_postal_code"].astype(str)

df_All=df_All.merge(df_unique,on="adress_postal_code",how='left')

# DF pour les stats de la semaine précédente 
#Pour le df Opération 
df_All['date'] = pd.to_datetime(df_All['date_of_signature_by_investor'], unit='ms')
aujourd_hui = datetime.today().date()
df_All['date'] = df_All['date'].dt.date

#Df des 7 jours
semaine_precedente = aujourd_hui - timedelta(days=7)
df_filtre_opé = df_All[df_All['date'] >= semaine_precedente]

#DF permettant de faire la comparaison de la fonction repeat  
df_no_7d = df_All[df_All['date'] <= semaine_precedente]

#DF avec les produits par opé et investor_id 7jours
repeat_investors_subset = df_filtre_opé[['investor_id', 'p_category', 'p_slug']]

#pas plot 
products_dict = {}
for investor_id, group in repeat_investors_subset.groupby('investor_id'):
    products_dict[investor_id] = list(group[['p_category', 'p_slug']].to_records(index=False))
df_product_7d = pd.DataFrame.from_dict(products_dict, orient='index', columns=['opération_1', 'opération_2'])

#Pour le df 14 jours et 7 jours des opés
semaine_precedente_x2 = semaine_precedente - timedelta(days=7)
df_filtre_opé_x2 = df_All[(df_All['date'] >= semaine_precedente_x2) & (df_All['date'] <= semaine_precedente)]

#DF avec les produits par opé et investor_id 14jours
repeat_investors_subset_14 = df_filtre_opé_x2[['investor_id', 'p_category', 'p_slug']]
products_dict_14 = {}
for investor_id, group in repeat_investors_subset_14.groupby('investor_id'):
    products_dict_14[investor_id] = list(group[['p_category', 'p_slug']].to_records(index=False))
df_product_14d = pd.DataFrame.from_dict(products_dict_14, orient='index', columns=['opération_1', 'opération_2'])

#DF sans les 14 derniers jours 
df_no_14d = df_All[df_All['date'] <= semaine_precedente_x2]

#Df meilleur produit 7 jours
df_top_product_opé=df_filtre_opé.groupby('p_category')["total_amount"].sum().sort_values(ascending=False).head(1)
df_top_product_opé=pd.DataFrame(df_top_product_opé).reset_index()

#Df meilleur produit 14 jours
df_top_product_opé_14=df_filtre_opé_x2.groupby('p_category')["total_amount"].sum().sort_values(ascending=False).head(1)
df_top_product_opé_14=pd.DataFrame(df_top_product_opé_14).reset_index()

#Df pour la carte des investisseurs
df_map=pd.DataFrame(df_All.groupby('adress_postal_code')['investor_id'].count())
df_map.reset_index( inplace=True)

#Merge qui drop les adresses de personnes morales 
df_map["adress_postal_code"]=df_map["adress_postal_code"].astype(str)
df_map=df_All.merge(df_map,on="adress_postal_code",how='left')

#DF map sans les NaN 
df_carte = df_map[['longitude',"latitude","investor_id_y"]].dropna()

#Carte pour les produits d'analyse totale 
map_df = gpd.read_file("http://osm13.openstreetmap.fr/~cquest/openfla/export/departements-20180101-shp.zip")

#A revoir pas les DOMTOM 
mapdf_nodom = map_df[ ~map_df.code_insee.str.contains("\d\d\d") ]

#Fonction pour avoir la colonne avec les range d'âge
def categorize_age(age):
    if age < 30:
        return "Moins de 30 ans"
    elif 31 <= age <= 45:
        return "De 31 à 45 ans"
    elif 46 <= age <= 59:
        return "De 46 à 59 ans"
    else:
        return "60 ans et plus"

# Appliquer la fonction pour créer une nouvelle colonne 'AgeGroup'
df_All['AgeGroup'] = df_All['Age'].apply(categorize_age)

#DF pour obtenir le meilleur produit sur la somme investie 
df_top_product_All=df_All.groupby('p_category')["total_amount"].sum().sort_values(ascending=False).head(1)
df_top_product_All=pd.DataFrame(df_top_product_All).reset_index()

# Affiche la page sélectionnée
if choix_page_sidebar == 'Analyse totale & 7 jours':

    #PAGE 1
    st.title('Dashboard')
    st.markdown("#")
    st.subheader("KPI overall")

    #Stats of the body 
    Total_Investment_All=int(df_operations_valide["total_amount"].sum())
    Average_Investment_All=int(df_operations_valide["total_amount"].mean())
    Count_Investment_All=int(df_operations_valide["total_amount"].count())
    Top_product_All=df_top_product_All.loc[0,'p_category']

    #Pourcentage des tranches d'âge
    percentage_distribution = df_All.AgeGroup.value_counts(normalize=True) * 100
    age_df = pd.DataFrame(list(percentage_distribution.items()), columns=['Age', 'Percentage'])

    #CSS for the stats 
    total1,total5, total4, total2=st.columns(spec=[0.3,0.24,0.24,0.24],gap='large')
    with total1:
        st.info('Total Invest', icon="📈")
        st.metric(label="Amount in €",value=f"{Total_Investment_All:,.0f}" )
    with total5:
        st.info('Goat', icon='📈')
        st.metric(label="Top produit",value=f"{Top_product_All}")
    with total2:
        st.info('Average', icon='📈')
        st.metric(label="Average mean €",value=f"{Average_Investment_All:,.0f}")
    with total4:
        st.info('Count', icon='📈')
        st.metric(label="Count ope",value=f"{Count_Investment_All:,.0f}")

    st.markdown("#")

    st.subheader("Graphiques")

    #graphique du nombre d'investisseurs par semaine 
    df_All['date'] = pd.to_datetime(df_All['date'])
    weekly_counts = df_All.resample('W-MON', on='date')['investor_id'].count()

    #graphique de l'investissement par semaine 
    df_All['date'] = pd.to_datetime(df_All['date'])
    weekly_total = df_All.resample('W-MON', on='date')['total_amount'].sum()

    ### Montant moyen investis (chiffre) → sur la totalité du temps
    mean_amount=df_All.resample('W-MON', on='date')["total_amount"].mean()

    #1. 📈 Nombre d'investisseurs par semaine
    st.subheader("Nombre d'investisseurs par semaine")
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(weekly_counts.index, weekly_counts.values, marker='o', linestyle='-', color='b')
    ax1.set_xlabel('Semaine')
    ax1.set_ylabel("Nombre d'investisseurs")
    ax1.set_title("Évolution du nombre d'investisseurs")
    ax1.grid(True)
    plt.xticks(rotation=45)
    st.pyplot(fig1)

    # 2. 💶 Montant total investi par semaine
    st.subheader("Montant total investi par semaine (€)")
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.plot(weekly_total.index, weekly_total.values, marker='o', linestyle='-', color='c')
    ax2.set_xlabel('Semaine')
    ax2.set_ylabel("Montant total (€)")
    ax2.set_title("Évolution des montants investis")
    ax2.grid(True)
    plt.xticks(rotation=45)
    st.pyplot(fig2)

    # 3. 📊 Montant moyen investi par semaine
    st.subheader("Montant moyen investi par semaine (€)")
    fig3, ax3 = plt.subplots(figsize=(10, 6))
    ax3.plot(mean_amount.index, mean_amount.values, marker='o', linestyle='-', color='#344D59')
    ax3.set_xlabel('Semaine')
    ax3.set_ylabel("Montant moyen (€)")
    ax3.set_title("Évolution du montant moyen investi")
    ax3.grid(True)
    plt.xticks(rotation=45)
    st.pyplot(fig3)

    st.caption("Ticket moyen par produits d'épargne") 
    mean_ticket_category=df_operat_valide_merged.groupby("p_category")["total_amount"].mean()
    st.bar_chart(mean_ticket_category)

    st.caption("Moyenne des notes par produits d'épargne") 
    rate_category=df_operat_valide_merged.groupby("p_category")["rating"].mean()
    st.bar_chart(rate_category)

    #Nombre d'utilisateur par semaine
    weekly_counts_utili = df_utilisateur.groupby('createdAt')['information_id'].count()
    # Tracer le graphique du nombre d'investisseurs par semaine

    st.caption("Evolution du nombre d'utilisteur")
    st.area_chart(weekly_counts_utili)

    st.subheader("Les profils sociaux")
    st.markdown("#")

    #Pourcentage sexe dans un tableau pour les investisseurs
    percentage_sexe = df_All.gender.value_counts(normalize=True) * 100
    Sexe_df = pd.DataFrame(list(percentage_sexe.items()), columns=['Sexe', 'Percentage'])
    Sexe=Sexe_df.sort_values(by='Sexe', ascending=False)

    # Créez le graphique à barres verticales avec Plotly Express
    fig = px.bar(Sexe, x='Sexe', y='Percentage', color='Sexe', labels={'Pourcentage': 'Percentage'})

    # Personnalisez le titre et les couleurs si nécessaire
    fig.update_layout(title='Répartition Femmes/Hommes', xaxis_title='Sexe', yaxis_title='Percentage')

    # Affichez le graphique
    st.plotly_chart(fig)

    #Répartition des tranches d'âge
    plt.figure(figsize=(8, 6))
    plt.pie(age_df["Percentage"],radius=1, labels=age_df["Age"], autopct='%1.1f%%', startangle=90, colors=['#FF9999', '#66B3FF', '#99FF99', '#FFCC99'])
    plt.title("Répartition des tranches d'âge")
    st.pyplot(plt)

    st.markdown("#")

    # CSP (Classes socio-pro) (camembert + autres idées)
    percentage_CSP1 = df_All.profession_category.value_counts(normalize=True) * 100
    CSP_df1 = pd.DataFrame(list(percentage_CSP1.items()), columns=['CSP', 'Percentage'])

    percentage_CSP2 = df_All.profession.value_counts(normalize=True) * 100
    CSP_df2 = pd.DataFrame(list(percentage_CSP2.items()), columns=['CSP', 'Percentage'])
    CSP2=CSP_df2.sort_values(by='Percentage', ascending=False)

    st.caption("Top 3 des profession_category")
    st.table(CSP_df1.head(3))

    st.caption("Top 3 des professions")
    st.table(CSP2.head(3))

    st.subheader("Les produits")
    st.markdown("#")

    #Produits les plus souscrits
    df_p_category = df_operat_valide_merged.p_category.value_counts(normalize=True) * 100
    df_pourcent_operat_merged = pd.DataFrame(list(df_p_category.items()), columns=['Category', 'Percentage'])

    st.caption("Produits les plus souscrits")
    st.table(df_pourcent_operat_merged)

    #Carte des produits les plus popen France 
    grouped = df_All.groupby("département_code")["p_category"].value_counts()
    grouped=pd.DataFrame(grouped).reset_index()
    sorted_df = grouped.sort_values(by=['département_code', 'count'], ascending=False)

    #Supprimez les doublons de département (conservez uniquement la première occurrence)
    result = sorted_df.drop_duplicates(subset='département_code', keep='first')
    result["code_insee"]=result["département_code"]
    df_prod_reg=result.merge(mapdf_nodom,on='code_insee')
    
    #Top produits par départements
    df_prod_reg_gpd = gpd.GeoDataFrame(df_prod_reg, geometry='geometry', crs='EPSG:4326')
    ig, ax = plt.subplots(figsize=(10, 10))
    df_prod_reg_gpd.plot(column='p_category', cmap='RdYlBu_r', legend=True, ax=ax)
    plt.title('Top produits par départements')
    plt.axis('off')  # Masquez les axes

    st.subheader("Cartes")
    st.pyplot(plt)

    st.subheader('Carte des investisseurs')
    st.map(df_carte,latitude="latitude",longitude='longitude', size="investor_id_y", color='#0044ff')

    st.markdown("#")

    st.subheader("KPI sur 7 jours")

    #Partie des 7 jours :

    #Création de la colonne repeat/ new pour les invest
    all_except_7_days_ids = set(df_no_7d["investor_id"])

    df_filtre_opé['repeat_or_new'] = df_filtre_opé["investor_id"].apply(
            lambda x: 'repeat' if x in all_except_7_days_ids else 'new'
        )

    #DF nombre total d'invest par investisseur 7 jours 
    df_filtre_final=pd.DataFrame(df_filtre_opé.groupby("investor_id")['total_amount'].sum())
    df_filtre_final=pd.DataFrame(df_filtre_final)
    df_filtre_final.reset_index(inplace=True)

    #DF nombre total d'opé par investisseur 7 jours 
    df_filtre_count=pd.DataFrame(df_filtre_opé.groupby("investor_id")['total_amount'].count())
    df_filtre_count.rename(columns={'total_amount': 'number_ope'}, inplace=True)
    df_filtre_count.reset_index( inplace=True)

    #DF produit et sous-produit investient par investisseur 7 jours
    df_product_7d = pd.DataFrame.from_dict(products_dict, orient='index', columns=['opération_1', 'opération_2'])
    df_product_7d.reset_index(inplace=True)
    df_product_7d.rename(columns={'index': 'investor_id'}, inplace=True)

    #Merge des 3 DF : investor-id / Nombre d'opé / total invest / repeat|new / prod invest
    df_filtre_final=df_filtre_final.merge(df_filtre_count,on="investor_id")
    df_filtre_final_merge=df_filtre_final.merge(df_filtre_opé[["investor_id","repeat_or_new"]],on="investor_id")
    df_filtre_final_merge=df_filtre_final_merge.drop_duplicates()
   
    df_filtre_final_merge_=df_product_7d.merge(df_filtre_final_merge,on="investor_id",how='inner')
    df_filtre_final_merge_["total_amount"]= df_filtre_final_merge_["total_amount"].astype(int)


    #Création de la colonne repeat/ new pour les invest 14 à 7 jours  
    all_except_14_days_ids = set(df_no_14d["investor_id"])

    df_filtre_opé_x2['repeat_or_new'] = df_filtre_opé_x2["investor_id"].apply(
            lambda x: 'repeat' if x in all_except_14_days_ids else 'new'
        )
    #DF nombre total d'invest par investisseur 14 jours 
    df_filtre_final_14=pd.DataFrame(df_filtre_opé_x2.groupby("investor_id")['total_amount'].sum())
    df_filtre_final_14=pd.DataFrame(df_filtre_final_14)
    df_filtre_final_14.reset_index(inplace=True)

    #DF nombre total d'opé par investisseur 14 jours 
    df_filtre_count_14=pd.DataFrame(df_filtre_opé_x2.groupby("investor_id")['total_amount'].count())
    df_filtre_count_14.rename(columns={'total_amount': 'number_ope'}, inplace=True)
    df_filtre_count_14.reset_index( inplace=True)

    #DF produit et sous-produit investient par investisseur 14 jours
    df_product_14d = pd.DataFrame.from_dict(products_dict_14, orient='index', columns=['opération_1', 'opération_2'])
    df_product_14d.reset_index(inplace=True)
    df_product_14d.rename(columns={'index': 'investor_id'}, inplace=True)

    #Merge des 3 DF 14 jours : investor-id / Nombre d'opé / total invest / repeat|new / prod invest
    df_filtre_final_14=df_filtre_final_14.merge(df_filtre_count_14,on="investor_id")
    df_filtre_final_merge_14=df_filtre_final_14.merge(df_filtre_opé_x2[["investor_id","repeat_or_new"]],on="investor_id")
    df_filtre_final_merge_14=df_filtre_final_merge_14.drop_duplicates()
   
    df_filtre_final_merge_14=df_product_14d.merge(df_filtre_final_merge_14,on="investor_id",how='inner')
    df_filtre_final_merge_14["total_amount"]= df_filtre_final_merge_14["total_amount"].astype(int)

    #7 jours
    Total_Investment=int(df_filtre_opé["total_amount"].sum())
    Average_Investment=int(df_filtre_opé["total_amount"].mean())
    Count_Investment=int(df_filtre_opé["total_amount"].count())
    Top_product_opé=df_top_product_opé.loc[0,'p_category']

    #14 - 7 jours
    Total_Investment_14=int(df_filtre_opé_x2["total_amount"].sum())
    Average_Investment_14=int(df_filtre_opé_x2["total_amount"].mean())
    Count_Investment_14=int(df_filtre_opé_x2["total_amount"].count())
    Top_product_opé_14=df_top_product_opé_14.loc[0,'p_category']

    #7 jours repeat / new
    repeat_count = len(df_filtre_final_merge_[df_filtre_final_merge_["repeat_or_new"] == 'repeat'])
    new_count = len(df_filtre_final_merge_[df_filtre_final_merge_["repeat_or_new"] == 'new'])

    #14 jours repeat / new
    repeat_count_14 = len(df_filtre_final_merge_14[df_filtre_final_merge_14["repeat_or_new"] == 'repeat'])
    new_count_14 = len(df_filtre_final_merge_14[df_filtre_final_merge_14["repeat_or_new"] == 'new'])


    #Données des 7 jours 
    total1,total2, total3 =st.columns(spec=[0.33,0.33,0.33],gap='large')  

    with total1:
        st.info('Total Invest', icon="📈")
        st.metric(label=str(Total_Investment_14),value=f"{Total_Investment:,.0f}" )
    with total2:
        st.info('Top product', icon='📈')
        st.metric(label=str(Top_product_opé_14),value=f"{Top_product_opé}")
    with total3:
        st.info('Average', icon='📈')
        st.metric(label=str(Average_Investment_14),value=f"{Average_Investment:,.0f}")
    
    total1,total2, total3 =st.columns(spec=[0.33,0.33,0.33],gap='large')

    with total1:
        st.info('Operation', icon='📈')
        st.metric(label=str(Count_Investment_14),value=f"{Count_Investment:,.0f}")
    with total2:
        st.info('Repeat', icon="📈")
        st.metric(label=str(repeat_count_14),value=f"{repeat_count:,.0f}" )
    with total3:
        st.info('New', icon='📈')
        st.metric(label=str(new_count_14),value=f"{new_count}")
    st.markdown("""---""")
    
    #Satistiques pour les p_category 7 jours 
    montant_total_par_category = df_filtre_opé.groupby('p_category')['total_amount'].sum()
    montant_total_par_category=pd.DataFrame(montant_total_par_category).reset_index()

    montant_ope_cat=df_filtre_opé.groupby('p_category')["total_amount"].count()
    montant_ope_cat=pd.DataFrame(montant_ope_cat).reset_index()
    montant_ope_cat=montant_ope_cat.rename(columns={'total_amount': 'Operations'})

    montant_total_par_category=montant_total_par_category.merge(montant_ope_cat,on='p_category')

    montant_total_par_category["average_basket"]=montant_total_par_category.apply(lambda row: row["total_amount"] / row["Operations"], axis=1)
    montant_total_par_category["average_basket"]=montant_total_par_category["average_basket"].astype(int)
    montant_total_par_category["total_amount"]=montant_total_par_category["total_amount"].astype(int)


    #Satistiques pour les p_category 7 à 14 jours 
    montant_total_par_category_x2 = df_filtre_opé_x2.groupby('p_category')['total_amount'].sum()
    montant_total_par_category_x2=pd.DataFrame(montant_total_par_category_x2).reset_index()

    montant_ope_cat_x2=df_filtre_opé_x2.groupby('p_category')["total_amount"].count()
    montant_ope_cat_x2=pd.DataFrame(montant_ope_cat_x2).reset_index()
    montant_ope_cat_x2=montant_ope_cat_x2.rename(columns={'total_amount': 'Operations'})

    montant_total_par_category_x2=montant_total_par_category_x2.merge(montant_ope_cat_x2,on='p_category')

    montant_total_par_category_x2["average_basket"]=montant_total_par_category_x2.apply(lambda row: row["total_amount"] / row["Operations"], axis=1)
    montant_total_par_category_x2["average_basket"]=montant_total_par_category_x2["average_basket"].astype(int)
    montant_total_par_category_x2["total_amount"]=montant_total_par_category_x2["total_amount"].astype(int)


    #statistiques pour les p_slug
    montant_total_par_slug = df_filtre_opé.groupby('p_slug')['total_amount'].sum()
    montant_total_par_slug=pd.DataFrame(montant_total_par_slug).reset_index()

    montant_ope_slug=df_filtre_opé.groupby('p_slug')["total_amount"].count()
    montant_ope_slug=pd.DataFrame(montant_ope_slug).reset_index()
    montant_ope_slug=montant_ope_slug.rename(columns={'total_amount': 'Operations'})

    montant_total_par_slug=montant_total_par_slug.merge(montant_ope_slug,on='p_slug')

    montant_total_par_slug["average_basket"]=montant_total_par_slug.apply(lambda row: row["total_amount"] / row["Operations"], axis=1)
    montant_total_par_slug["average_basket"]=montant_total_par_slug["average_basket"].astype(int)
    montant_total_par_slug["total_amount"]=montant_total_par_slug["total_amount"].astype(int)

    #Création de la colonne creation de la discussion en Y M D
    df_forums['date_creat_discussion'] = pd.to_datetime(df_forums['createdAt'], unit='ms')
    df_forums['date_creat_discussion'] = df_forums['date_creat_discussion'].dt.date

    #Satistiques pour df_forums 
    df_forums['date_rep'] = pd.to_datetime(df_forums['last_response_date'], unit='ms')
    df_forums['date_rep'] = df_forums['date_rep'].dt.date

    df_filtre_rep = df_forums[df_forums['date_rep'] >= semaine_precedente]
    df_filtre_rep=df_filtre_rep[["title","nb_responses","nb_views","date_creat_discussion"]]
    
    st.subheader("Tableaux détaillés des 7 derniers jours")

    #streamlit partie
    st.caption("### Les produits")
    st.table(montant_total_par_category.sort_values(by="total_amount",ascending=False))

    st.caption("### Les sous produits")
    st.table(montant_total_par_slug.sort_values(by="total_amount",ascending=False).head(7))

    st.caption("Total des montants investis par p_slug")

    #montant_total_par_slug
    fig = px.bar(montant_total_par_slug, x='p_slug', y='total_amount', color='Operations', labels={'total_amount': 'Total Amount'})

    # Ajoutez des étiquettes pour afficher le nombre d'opérations au survol
    fig.update_traces(texttemplate='%{y}', textposition='outside')

    # Affichez le graphique dans Streamlit
    st.plotly_chart(fig)

    st.markdown("""---""")

    st.caption("### Les forums")
    st.table(df_filtre_rep.sort_values(by="date_creat_discussion",ascending=False))

    st.markdown("""---""")

    #plot du DF sur les repeat ect 
    st.caption("### Les investisseurs")
    st.table(df_filtre_final_merge.sort_values(by="repeat_or_new",ascending=False))

    st.markdown("""---""")

    st.subheader("KPI du 14 au 7 ième jour")
    #CSS for the stats 14 jours 
    total1,total2, total3 =st.columns(spec=[0.33,0.33,0.33],gap='large')  

    with total1:
        st.info('Total Invest', icon="📈")
        st.metric(label="Amount in €",value=f"{Total_Investment_14:,.0f}" )
    with total2:
        st.info('Goat', icon='📈')
        st.metric(label="Top produit",value=f"{Top_product_opé_14}")
    with total3:
        st.info('Average', icon='📈')
        st.metric(label="Average mean €",value=f"{Average_Investment_14:,.0f}")
    
    total1,total2, total3 =st.columns(spec=[0.33,0.33,0.33],gap='large')

    with total1:
        st.info('Count', icon='📈')
        st.metric(label="Count ope",value=f"{Count_Investment_14:,.0f}")
    with total2:
        st.info('Repeat', icon="📈")
        st.metric(label="Amount of repeat",value=f"{repeat_count_14:,.0f}" )
    with total3:
        st.info('New', icon='📈')
        st.metric(label="Amount of new",value=f"{new_count_14}")

else:

    st.title('Dashboard')
    st.subheader("Analyse libre") 
    
    # Sélectionner la période temporelle
    st.sidebar.header("Select a range of dates")
    start_date = st.sidebar.date_input("Start date", pd.to_datetime("2023/12/01").date())
    end_date = st.sidebar.date_input("End date", pd.to_datetime('today').date())

    def generate_date_list(start_date, end_date):
        date_range = pd.date_range(start=start_date, end=end_date)
        return pd.to_datetime(date_range).tolist()
    
    Date=generate_date_list(start_date, end_date)


    st.sidebar.header("select filter")

    #Filtres applicables:
    Gender=st.sidebar.multiselect(
            "Select a gender",
            options=df_All["gender"].unique(),
            default=df_All["gender"].unique()   
            )
    
    Category=st.sidebar.multiselect(
            "Select a p_category",
            options=df_All["p_category"].unique(),
            default=df_All["p_category"].unique()   
            )
    
    age=st.sidebar.multiselect(
            "Select a Age",
            options=df_All["AgeGroup"].unique(),
            default=df_All["AgeGroup"].unique()  
            )

    Profession_category=st.sidebar.multiselect(
                "Select a profession_category",
                options=df_All["profession_category"].unique(),
                default=df_All["profession_category"].unique()   
            )
    Depart= st.sidebar.multiselect(
            "Select a département_code",
            options=df_All["département_code"].sort_values(ascending=False).unique(),
            default=df_All["département_code"].unique()
            )
    
    P_slug= st.sidebar.multiselect(
            "Select a p_slug",
            options=df_All["p_slug"].unique(),
            default=df_All["p_slug"].unique()   
            )
    
    #range d'invest filtre
    def generate_integer_list2(min_value, max_value):
        return list(range(min_value, max_value + 1))

    st.write("Sélectionner une range d'€ d'investissement")
    min_value = st.number_input("Valeur min €", min_value=0, value=int(df_All["total_amount"].min()))
    max_value = st.number_input("Valeur max €", min_value=min_value, value=int(df_All["total_amount"].max()))

    # Générer la liste d'entiers d'invest
    Total_amount = generate_integer_list2(min_value, max_value)

    #Df dynamique avec les filtres
    df_selection=df_All.query("gender==@Gender & département_code==@Depart & p_category==@Category & profession_category==@Profession_category & p_slug==@P_slug & AgeGroup==@age"
                              )
    #Df dynamique avec le filtre invest
    df_selection=df_selection[df_selection['total_amount'].between(min_value, max_value)]

    st.markdown("##")

    #DF pour obtenir le meilleur produit par rapport à la somme investie 
    df_top_product_A=df_selection.groupby('p_category')["total_amount"].sum().sort_values(ascending=False).head(1)
    df_top_product_A=pd.DataFrame(df_top_product_A).reset_index()

    #Stats of the body 
    Total_Investment_A=int(df_selection["total_amount"].sum())
    Average_Investment_A=int(df_selection["total_amount"].mean())
    Count_Investment_A=int(df_selection["total_amount"].count())
    Top_product_A=df_top_product_A.loc[0,'p_category']

    #Df pour la map page 2 
    df_map_select=pd.DataFrame(df_selection.groupby('adress_postal_code')['investor_id'].count())
    df_map_select.reset_index( inplace=True)

    df_map_select["adress_postal_code"]=df_map_select["adress_postal_code"].astype(str)
    df_map_select=df_map_select.merge(df_selection,on="adress_postal_code",how='left')

    df_carte_select = df_map_select[['longitude',"latitude","investor_id_y"]].dropna()
    df_carte["investor_id_y"]=df_carte["investor_id_y"].astype(int)



    #CSS for the stats 
    total1,total5, total4, total2=st.columns(spec=[0.3,0.24,0.24,0.24],gap='large')
    with total1:
        st.info('Total Invest', icon="📈")
        st.metric(label="Amount in €",value=f"{Total_Investment_A:,.0f}" )
    with total5:
        st.info('Goat', icon='🎯')
        st.metric(label="Top produit",value=f"{Top_product_A}")
    with total2:
        st.info('Average', icon='📈')
        st.metric(label="Average mean €",value=f"{Average_Investment_A:,.0f}")
    with total4:
        st.info('Count', icon='📈')
        st.metric(label="Count ope",value=f"{Count_Investment_A:,.0f}")

    #statistiques pour les p_category 
    montant_total_par_category_select = df_selection.groupby('p_category')['total_amount'].sum()
    montant_total_par_category_select=pd.DataFrame(montant_total_par_category_select).reset_index()

    montant_ope_cat_select=df_selection.groupby('p_category')["total_amount"].count()
    montant_ope_cat_select=pd.DataFrame(montant_ope_cat_select).reset_index()
    montant_ope_cat_select=montant_ope_cat_select.rename(columns={'total_amount': 'Operations'})

    montant_total_par_category_select=montant_total_par_category_select.merge(montant_ope_cat_select,on='p_category')

    montant_total_par_category_select["average_basket"]=montant_total_par_category_select.apply(lambda row: row["total_amount"] / row["Operations"], axis=1)
    montant_total_par_category_select["average_basket"]=montant_total_par_category_select["average_basket"].astype(int)
    montant_total_par_category_select["total_amount"]=montant_total_par_category_select["total_amount"].astype(int)


    #statistiques pour les p_slug
    montant_total_par_slug_select = df_selection.groupby('p_slug')['total_amount'].sum()
    montant_total_par_slug_select=pd.DataFrame(montant_total_par_slug_select).reset_index()

    montant_ope_slug_select=df_selection.groupby('p_slug')["total_amount"].count()
    montant_ope_slug_select=pd.DataFrame(montant_ope_slug_select).reset_index()
    montant_ope_slug_select=montant_ope_slug_select.rename(columns={'total_amount': 'Operations'})

    montant_total_par_slug_select=montant_total_par_slug_select.merge(montant_ope_slug_select,on='p_slug')

    montant_total_par_slug_select["average_basket"]=montant_total_par_slug_select.apply(lambda row: row["total_amount"] / row["Operations"], axis=1)
    montant_total_par_slug_select["average_basket"]=montant_total_par_slug_select["average_basket"].astype(int)
    montant_total_par_slug_select["total_amount"]=montant_total_par_slug_select["total_amount"].astype(int)

    st.markdown("### Statistiques sur les produits")
    st.table(montant_total_par_category_select.sort_values(by="total_amount",ascending=False))

    st.markdown("### Statistiques sur le top 5 des sous produits")
    st.table(montant_total_par_slug_select.sort_values(by="total_amount",ascending=False).head(5))

    #Carte Investisseur 
    st.markdown("""---""")
    st.subheader("Carte des investisseurs")
    st.map(df_carte_select,latitude="latitude",longitude='longitude', size="investor_id_y", color='#0044ff')

    st.markdown("##")

    pyg_app = StreamlitRenderer(df_selection)
    
    if st.button('Do you want to explore the dataset deeper ?'):
        st.write('You can start your own analysis')
        pyg_app.explorer()



#Final tache  
#Revoir le graphe des utilisateurs par semaine 

#BUG 
#Analyse libre : Problème sur le filtre date /hypothèse les dates de la liste se transforme en string et ne peuvent pas être retrouvées dans le DF car elles ne sont pas du même type que celles dans le DF alors il match pas 

#7 jours 
#Faire plus des charts que des tableaux 

#Graph de comparaison avec la semaine précedente et d'il y a deux semaines
#statistiques pour les p_category 

#ANALYSE LIBRE
#Rajouter des graphes sur le montant total 
# Ajouter un filtre calendrier pour sélectionner les périodes souhaitées + BONUS rajouter une feature permettant de sélectionner les années souhaités pour les voir superporsés avec les autres 

#ANALYSE TOTALE 
#Mettre un camenbert des ages des utilisateurs / des investisseurs
#Faire une line chart de l'evo des souscriptions / montant total
#Tableau pour les moins de 30 ans / les plus de 60 ans / Les femmes + graphes représenatnt le montant total en abs avec les 5 produits en diff couleurs 

#Faire une range de 30 jurs avec yun graphique avec toutes les courbes des mois représentés de chaque couleur différente
 
#IDEE DE GRAPHES 
#Nombre de Souscritions par prod dans le temps (line chart)
#Evo du montant total pour chaque prod 
#Evo des genres dans le temps (invest)
#Evo des ages dans le temps (Bar chart sur 100 %, invest)


##DONE##
#Mettre une comparaison avec les 14 derniers jours pour comparer avec la semaine précedente : faire une graphe avec la superposition des deux semaines 
#Affihcer le total repeat et le total opé en grand 7jours 
#Refaire les KPI's pour les 7 - 14 jours 
#NOMBRE DE UTILISATEUR PAR SEMAINE a faire / Porblème sur Created At fonction 
#Le dicytionnaire des produits et des sous produits des 7 jours n'est pas optimisée, si il y a 3 opé, cela fonctionnera pas 
#Rajouter un tableau avec pour chaque ligne l'id des mecs : investor_id +  le nombre en total amount depensé + le volume + si c'est un repeat + le p_slug / produit 
#mettre une sous section sur si un investisseur est un repeat ou nouveau en checkant avec une fonction si une opé a déjà était validé avec le meme id utilisateur 
#Faire la fonction repaeat / créer un dataframe sans les valeurs de les 7 jours 
#Gérer le bordel de df_map
#Problème sur une des opé de crypto = problème sur le query pour l'analyse libre
#problème sur l'analyse libre / On dirait que le Query fonctionne mal 
#A faire en prio les df sont pas les bons plots il faut revorir ca en PRIO 
#Forum : nombre de discussion totale et de nouvelles discus : "createdAt" / nbr total de mess et les nouveaux : "last_response_date" / problème les discussions sont actualisées donc on ne peut pas savoir kles disqcussions nouvelles : solution : plot les discussions et rep récentes 
#Faire la map avec le top prod dans chaque departement 
#Graphe du totale montant sur l'année 
#faire la map avec les produits pref dans chaque departement 
#Le df filtre des 7 jours double un ligne problème étrange 
#Problème avec le CreatedAt et donc la fonction nbr utili par semaine 
#Faire sur les 7 derniers jours : afficher les stats sur le montant total / le nombre d'investis/ le nbr d'opé valide 
#Mettre le count sur les 7 derniers jours du nmbr d'évaluation sur tous produits confondues 
#Crée une deuxième page pour les analyses sur 7 jours et laisser une page les analyses 'libres' et 3 ième page stats importantes
#faire deux tableau un pour p_category et un pour p_slug sur les stats avec le montant investi / le count / panier moyen 
#Faire pour les âges un curseur sur le coté pour sélectionner les tranches d'âge pareil pour le montant investi 
#Changer la map pour avoir le nombre de pers par ppints + 
#Rajouter Pygwlaker à la fin pour analyse 
#Mauvais df select pour le dF_all régler ca PRIO = problème vient des codes postal des personnes morales 
#Changer le curseur des ages en les regroupant par tranche d'age par une fonction ou avec st.slider
#graphe des utilisateurs par semaine