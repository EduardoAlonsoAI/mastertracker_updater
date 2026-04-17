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
    # Filtro MX + Juarez + Mazatlan
    subregion_col = df.iloc[:, 0].astype(str).str.strip().str.upper()
    city_name_col = df.iloc[:, 3].astype(str).str.strip()
    
    mask = (subregion_col == 'MX') | (city_name_col.str.contains('Juarez|Juárez|Mazatlan|Mazatlán', case=False, regex=True, na=False))
    df = df[mask].copy()
    
    # 1. Conservar solo hasta la columna AP (índice 41)
    df = df.iloc[:, :42].copy()
    
    # 2. Columna AG (Pax_active_cross, índice 32) en 0s
    df.iloc[:, 32] = 0
    
    # 3. Extraer valores limpios base
    city_col = df.iloc[:, 3].astype(str).str.strip() 
    date_col_series = pd.to_datetime(df.iloc[:, 5], errors='coerce') 
    
    # 4. Formatear la fecha base a yyyy-mm-dd
    df.iloc[:, 5] = date_col_series.dt.strftime('%Y-%m-%d')
    
    # --- LA TRAMPA ANTI-EXCEL PARA LA SEMANA Y EL AÑO ---
    def parse_year_week(val):
        val_str = str(val).strip()
        # Si Excel lo corrompió y lo volvió fecha "2026-12-01 00:00:00"
        if '-' in val_str and ':' in val_str:
            try:
                dt = pd.to_datetime(val_str)
                return dt.year, dt.month  # El "mes" en realidad es nuestra semana secuestrada
            except:
                return 0, 0
        # Si viene en el formato correcto "2026/14"
        elif '/' in val_str:
            parts = val_str.split('/')
            try:
                return int(parts[0]), int(parts[1])
            except:
                return 0, 0
        return 0, 0
        
    parsed_yw = df.iloc[:, 4].apply(parse_year_week)
    year = parsed_yw.apply(lambda x: x[0])
    week = parsed_yw.apply(lambda x: x[1])
    # ----------------------------------------------------
    
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

                    st.success(f"¡{uploaded_file.name} procesado! Listo para BigQuery.")

                    if st.button(f"🚀 Subir {uploaded_file.name} a BigQuery", key=f"bq_B_{uploaded_file.name}"):
                        with st.spinner("Transformando tipos de datos y subiendo a BigQuery..."):
                            try:
                                creds_dict = st.secrets["gcp_service_account"]
                                credentials = service_account.Credentials.from_service_account_info(creds_dict)
                                client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])
                                
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

                                processed_df_B.columns = [field.name for field in bq_schema]

                                # 4. CASTEO EXPLÍCITO CORREGIDO Y BLINDADO
                                for field in bq_schema:
                                    col = field.name
                                    if field.field_type == 'INTEGER':
                                        processed_df_B[col] = pd.to_numeric(processed_df_B[col], errors='coerce').astype('Int64')
                                    elif field.field_type == 'FLOAT':
                                        # Solo quitamos comas si la columna es detectada como texto ('object')
                                        if processed_df_B[col].dtype == 'object':
                                            processed_df_B[col] = processed_df_B[col].astype(str).str.replace(',', '', regex=False)
                                        processed_df_B[col] = pd.to_numeric(processed_df_B[col], errors='coerce').astype('float64')
                                    elif field.field_type == 'STRING':
                                        processed_df_B[col] = processed_df_B[col].astype(str).replace({'nan': '', 'NaN': '', 'None': ''})

                                job_config = bigquery.LoadJobConfig(
                                    schema=bq_schema,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                )
                                
                                table_id = 'didi_db.Burn SoT' 
                                
                                job = client.load_table_from_dataframe(
                                    processed_df_B, table_id, job_config=job_config
                                )
                                
                                job.result() 
                                
                                st.success("¡Subido con éxito a BigQuery! 🎉")
                            except Exception as e:
                                st.error(f"Error en BigQuery B: {e}")

                except Exception as e:
                    st.error(f"Error en proceso principal B: {e}")
