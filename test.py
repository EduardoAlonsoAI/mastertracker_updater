from google.oauth2 import service_account
from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="Master Tracker auto updater", layout="wide")

st.title("🚀 Master Tracker auto updater")
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
    
    # 4. --- AQUÍ ESTÁ EL CAMBIO, PADRE SANTO ---
    # En lugar de ponerle comas, obligamos a que sean números puros (floats)
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

def clean_for_bigquery(df):
    df_clean = df.copy()
    for col in df_clean.columns:
        # 1. Si es texto, intentar quitar comas y convertir a número puro
        if df_clean[col].dtype == 'object':
            try:
                temp = df_clean[col].apply(lambda x: str(x).replace(',', '') if pd.notnull(x) and isinstance(x, str) else x)
                df_clean[col] = pd.to_numeric(temp)
            except:
                pass # Si da error (ej. nombres de ciudades como "Querétaro"), se deja como texto
        
        # 2. Si es Float, lo limpiamos para que BigQuery no se confunda entre Ints y Floats
        if pd.api.types.is_float_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].apply(
                # f"{x:.10f}" evita la notación científica. rstrip quita los ceros y puntos inútiles al final.
                lambda x: f"{x:.10f}".rstrip('0').rstrip('.') if pd.notnull(x) else ""
            )
    return df_clean

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
                    # 2. Creamos el botón para enviar a BQ
                    if st.button(f"🚀 Subir {uploaded_file.name} a BigQuery", key=f"bq_A_{uploaded_file.name}"):
                        with st.spinner("Subiendo a BigQuery vía CSV..."):
                        
                            # 1. Jalamos las credenciales secretas
                            creds_dict = st.secrets["gcp_service_account"]
                            credentials = service_account.Credentials.from_service_account_info(creds_dict)
                            client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
                            
                            # 2. CONVERTIMOS EL DATAFRAME A UN ARCHIVO VIRTUAL (BYTES)
                            # Esto es lo que te estaba faltando para que no de error de "DataFrame"
                            csv_como_texto = processed_df_A.to_csv(index=False, header=False)
                            csv_como_bytes = csv_como_texto.encode('utf-8')
                            archivo_virtual = io.BytesIO(csv_como_bytes)
                            
                            # 3. Configuramos el trabajo para decirle a BigQuery que es un CSV puro
                            job_config = bigquery.LoadJobConfig(
                                source_format=bigquery.SourceFormat.CSV,
                                skip_leading_rows=0, 
                                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                            )
                            
                            # 4. ¡Enviamos el archivo directo a la tabla!
                            table_id = 'didi_db.Daily DB 100268' # <-- PON TU TABLA AQUÍ
                            job = client.load_table_from_file(archivo_virtual, table_id, job_config=job_config)
                            
                            job.result() # Esperamos a que BigQuery nos confirme que ya terminó
                        
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

                    # 2. ¡APLICAMOS LA VACUNA UNIVERSAL!
                    processed_df_B = clean_for_bigquery(processed_df_B)

                    st.success(f"¡{uploaded_file.name} procesado! Listo para BigQuery.")

                    # 2. Creamos el botón para enviar a BQ
                    # 2. Creamos el botón para enviar a BQ
                    if st.button(f"🚀 Subir {uploaded_file.name} a BigQuery", key=f"bq_B_{uploaded_file.name}"):
                        with st.spinner("Subiendo a BigQuery con Esquema Exacto..."):
                            try:
                                # 1. Jalamos las credenciales secretas
                                creds_dict = st.secrets["gcp_service_account"]
                                credentials = service_account.Credentials.from_service_account_info(creds_dict)
                                client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
                                
                                # 2. Definimos el esquema de BigQuery EXACTO que nos pasaste
                                bq_schema = [
                                    bigquery.SchemaField("subregion", "STRING"),
                                    bigquery.SchemaField("country_code", "STRING"),
                                    bigquery.SchemaField("city_id", "INTEGER"),
                                    bigquery.SchemaField("city_name", "STRING"),
                                    bigquery.SchemaField("year_calendar_week", "STRING"),
                                    bigquery.SchemaField("date_value", "DATE"),
                                    bigquery.SchemaField("product_id", "INTEGER"),
                                    bigquery.SchemaField("product_name", "STRING"),
                                    bigquery.SchemaField("gmv", "FLOAT"),
                                    bigquery.SchemaField("usd_fx", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_cx", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_mktp", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_fleet", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_shorterm", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_other", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_loyalty", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_captain", "INTEGER"),
                                    bigquery.SchemaField("drv_expan_guru", "FLOAT"),
                                    bigquery.SchemaField("drv_react_longterm", "FLOAT"),
                                    bigquery.SchemaField("drv_react_fleet", "INTEGER"),
                                    bigquery.SchemaField("drv_react_cbcamp", "FLOAT"),
                                    bigquery.SchemaField("drv_activ_newgi", "FLOAT"),
                                    bigquery.SchemaField("drv_activ_dormant", "FLOAT"),
                                    bigquery.SchemaField("drv_activ_fleet", "FLOAT"),
                                    bigquery.SchemaField("drv_activ_newcamp", "FLOAT"),
                                    bigquery.SchemaField("drv_activ_referral", "FLOAT"),
                                    bigquery.SchemaField("drv_activ_paidmkt", "FLOAT"),
                                    bigquery.SchemaField("pax_react_longterm", "FLOAT"),
                                    bigquery.SchemaField("pax_activ_new", "FLOAT"),
                                    bigquery.SchemaField("pax_activ_dormant", "FLOAT"),
                                    bigquery.SchemaField("pax_activ_referral", "FLOAT"),
                                    bigquery.SchemaField("pax_activ_paid_mkt", "FLOAT"),
                                    bigquery.SchemaField("pax_activ_cross", "INTEGER"),
                                    bigquery.SchemaField("pax_expan_mktp", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_other", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_employee", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_cx", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_didiclub", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_shorterm", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_riderpass", "FLOAT"),
                                    bigquery.SchemaField("pax_expan_surgepass", "FLOAT"),
                                    bigquery.SchemaField("drv_expan_driverpass", "FLOAT"),
                                    bigquery.SchemaField("CLUSTER", "STRING"),
                                    bigquery.SchemaField("WEEKNUM", "INTEGER"),
                                    bigquery.SchemaField("calendar_week", "INTEGER"),
                                    bigquery.SchemaField("year", "INTEGER"),
                                    bigquery.SchemaField("period", "STRING"),
                                    bigquery.SchemaField("weekday_name", "STRING"),
                                ]

                                # 3. Configuramos el Job con el esquema
                                job_config = bigquery.LoadJobConfig(
                                    schema=bq_schema,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                )
                                
                                # OJO: Los nombres de las columnas del DataFrame DEBEN ser iguales al esquema
                                # para usar el método load_table_from_dataframe
                                column_names = [field.name for field in bq_schema]
                                processed_df_B.columns = column_names

                                # 4. ¡Enviamos el DataFrame directo, guiado por el esquema!
                                table_id = 'didi_db.Daily DB 100268' # <-- PON TU TABLA AQUÍ
                                job = client.load_table_from_dataframe(
                                    processed_df_B, table_id, job_config=job_config
                                )
                                
                                job.result() # Esperamos a que BigQuery confirme
                                
                                st.success("¡Subido con éxito a BigQuery! 🎉")
                            except Exception as e:
                                st.error(f"Error en BigQuery: {e}")

                except Exception as e:
                    st.error(f"Error: {e}")
