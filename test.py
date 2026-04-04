from google.oauth2 import service_account
import pandas_gbq
import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="Master tracker updater", layout="wide")

st.title("🚀 BigQuery CSV Transformer")
st.markdown("Sube tus archivos CSV/XLSX. Navega entre las pestañas para elegir qué tabla quieres actualizar.")

# --- 1. Cargar Diccionarios ---
@st.cache_data
def load_dicts():
    try:
        abc_dict = pd.read_csv("ABC_dictionary.csv")
        d_dict = pd.read_csv("D_dictionary.csv")
        
        # Limpiar nombres de columnas por si traen espacios en blanco
        abc_dict.columns = abc_dict.columns.str.strip()
        d_dict.columns = d_dict.columns.str.strip()
        
        # Crear diccionarios de mapeo
        map_category = dict(zip(abc_dict['City name'], abc_dict['Category']))
        map_cluster = dict(zip(abc_dict['City name'], abc_dict['Cluster']))
        
        # Asegurar que la semana tenga dos dígitos (ej. '01', '02')
        d_dict['Week'] = d_dict['Week'].astype(str).str.zfill(2)
        map_period = dict(zip(d_dict['Week'], d_dict['Period']))
        
        return map_category, map_cluster, map_period
    except Exception as e:
        st.error(f"Error cargando diccionarios. Asegúrate de tenerlos en la misma carpeta: {e}")
        return None, None, None

map_category, map_cluster, map_period = load_dicts()

# --- 2. Lógicas de Transformación ---

# Transformación para CSV A
def process_dataframe_A(df, map_category, map_cluster, map_period):
    df = df.iloc[:, :31].copy()
    df.iloc[:, 29] = 0
    df.iloc[:, 30] = 0
    df.iloc[:, 2] = pd.to_datetime(df.iloc[:, 2], errors='coerce').dt.strftime('%Y-%m-%d')
    
     for col_idx in [26, 27, 28]:
        # Quitamos comas si es que el excel original ya las traía y lo convertimos a numérico
        df.iloc[:, col_idx] = df.iloc[:, col_idx].replace({',': ''}, regex=True)
        df.iloc[:, col_idx] = pd.to_numeric(df.iloc[:, col_idx], errors='coerce')
    
    city_col = df.iloc[:, 6].astype(str).str.strip()
    week_col = df.iloc[:, 1].astype(str).str.zfill(2)
    
    new_category = city_col.map(map_category).fillna("Other")
    new_cluster = city_col.map(map_cluster).fillna("Other")
    new_period = week_col.map(map_period).fillna("Unknown")
    
    df.insert(0, 'Category', new_category)
    df.insert(1, 'Cluster', new_cluster)
    df.insert(2, 'City Name Map', city_col)
    df.insert(3, 'Period', new_period)
    
    return df

# Transformación para CSV B
def process_dataframe_B(df, map_cluster, map_period):
    # --- NUEVO FILTRO BLINDADO ---
    subregion_col = df.iloc[:, 0].astype(str).str.strip().str.upper()
    city_name_col = df.iloc[:, 3].astype(str).str.strip()
    
    # Buscamos 'MX' en la subregión, o 'Juarez' / 'Juárez' en el nombre de la ciudad
    mask = (subregion_col == 'MX') | (city_name_col.str.contains('Juarez|Juárez', case=False, regex=True, na=False))
    df = df[mask].copy()
    
    # 1. Conservar solo hasta la columna AP (índice 41)
    df = df.iloc[:, :42].copy()
    
    # 2. Columna AG (Pax_active_cross, índice 32) en 0s
    df.iloc[:, 32] = "0"
    #df.iloc[:, 32] = df.iloc[:, 32].astype('int64')
    
    # 3. Extraer valores limpios base
    city_col = df.iloc[:, 3].astype(str).str.strip() 
    date_col_series = pd.to_datetime(df.iloc[:, 5], errors='coerce') 
    week_year_col = df.iloc[:, 4].astype(str).str.strip().str.split('/') 
    
    # 4. Formatear la fecha base a yyyy-mm-dd
    df.iloc[:, 5] = date_col_series.dt.strftime('%Y-%m-%d')
    
    # 5. Generar los datos de las nuevas columnas
    year = week_year_col.str[0].fillna(0).astype(int)
    week = week_year_col.str[1].fillna(0).astype(int)
    day_of_week = date_col_series.dt.day_name() 
    
    cluster = city_col.map(map_cluster).fillna("Other")
    period = week.astype(str).str.zfill(2).map(map_period).fillna("Unknown")
    
    # 6. Agregar las 6 columnas al final
    df['Cluster_AQ'] = cluster
    df['Weeknum_AR'] = week
    df['Weeknum_AS'] = week
    df['Year_AT'] = year
    df['Period_AU'] = period
    df['DayOfWeek_AV'] = day_of_week
    
    return df


# --- 3. Interfaz de Usuario (Pestañas) ---
if map_category is not None:
    
    tab1, tab2 = st.tabs(["📊 Updater Daily Metrics", "📈 Updater Burn SoT"])

    # --- PESTAÑA 1 ---
    with tab1:
        st.markdown("### 📥 Sube aquí el Daily Metrics (202406_SSL_MainMetricsWithManaged_di)")
        uploaded_files_A = st.file_uploader("Selecciona archivos", type=['csv', 'xlsx'], accept_multiple_files=True, key="uploader_A")

        if uploaded_files_A:
            for uploaded_file in uploaded_files_A:
                try:
                    # 1. Procesamos el archivo como ya lo teníamos
                    df_A = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file)
                    processed_df_A = process_dataframe_A(df_A, map_category, map_cluster, map_period)
                
                    st.success(f"¡{uploaded_file.name} procesado! Listo para BigQuery.")
                
                    # 2. Creamos el botón para enviar a BQ
                    if st.button(f"🚀 Subir {uploaded_file.name} a BigQuery", key=f"bq_A_{uploaded_file.name}"):
                        with st.spinner("Subiendo a BigQuery..."):
                        
                            # Jalamos las credenciales secretas de Streamlit Cloud
                            creds_dict = st.secrets["gcp_service_account"]
                            credentials = service_account.Credentials.from_service_account_info(creds_dict)
                        
                            # ¡La magia ocurre aquí!
                            pandas_gbq.to_gbq(
                                dataframe=processed_df_A,
                                destination_table='didi_db.Daily DB 100268', # <-- CAMBIA ESTO
                                project_id='valid-sol-477221-e8',                # <-- CAMBIA ESTO
                                if_exists='append',                        # <-- Le decimos que agregue (append)
                                credentials=credentials
                            )
                        st.success("¡Subido con éxito a BigQuery! 🎉")
                    
                except Exception as e:
                    st.error(f"Error: {e}")

    # --- PESTAÑA 2 ---
    with tab2:
        st.markdown("### 📥 Sube aquí el Burn SoT (202508_SoT_Burn_di)")
        uploaded_files_B = st.file_uploader("Selecciona archivos", type=['csv', 'xlsx'], accept_multiple_files=True, key="uploader_B")

        if uploaded_files_B:
            for uploaded_file in uploaded_files_B:
                try:
                    df_B = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file)
                    processed_df_B = process_dataframe_B(df_B, map_cluster, map_period)

                    st.success(f"¡{uploaded_file.name} procesado! Listo para BigQuery.")

                    # 2. Creamos el botón para enviar a BQ
                    if st.button(f"🚀 Subir {uploaded_file.name} a BigQuery", key=f"bq_A_{uploaded_file.name}"):
                        with st.spinner("Subiendo a BigQuery..."):

                            # Jalamos las credenciales secretas de Streamlit Cloud
                            creds_dict = st.secrets["gcp_service_account"]
                            credentials = service_account.Credentials.from_service_account_info(creds_dict)
                        
                            # ¡La magia ocurre aquí!
                            pandas_gbq.to_gbq(
                                dataframe=processed_df_B,
                                destination_table='didi_db.Daily DB 100268', # <-- CAMBIA ESTO
                                project_id='valid-sol-477221-e8',                # <-- CAMBIA ESTO
                                if_exists='append',                        # <-- Le decimos que agregue (append)
                                credentials=credentials
                            )

                        st.success("¡Subido con éxito a BigQuery! 🎉")

                except Exception as e:
                    st.error(f"Error: {e}")
