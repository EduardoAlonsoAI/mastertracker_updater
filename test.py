from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import datetime
import uuid
import io

# --- CONFIGURACIÓN DE PÁGINA Y UI ---
st.set_page_config(page_title="Data Ops | Master Tracker", page_icon="🚀", layout="wide")

col_head1, col_head2 = st.columns([3, 1])
with col_head1:
    st.title("🚀 Master Tracker Auto-Updater")
    st.markdown("Automatiza la ingesta, limpieza y carga de datos hacia BigQuery.")
with col_head2:
    st.link_button("📊 Ver en G-Sheets (Bridge)", "https://docs.google.com/spreadsheets/d/18q9hGVHkCwLyIrhavMoYGkxAU9ZMxuUTOS7RramILFc/edit?usp=sharing", use_container_width=True)

st.divider()

# --- 1. CARGA DE DICCIONARIOS ---
@st.cache_data
def load_dicts():
    try:
        abc_dict = pd.read_csv("ABC_dictionary.csv")
        d_dict = pd.read_csv("D_dictionary.csv")
        
        abc_dict.columns = abc_dict.columns.str.strip()
        d_dict.columns = d_dict.columns.str.strip()
        
        map_category = dict(zip(abc_dict['City name'], abc_dict['Category']))
        map_cluster = dict(zip(abc_dict['City name'], abc_dict['Cluster']))
        
        d_dict['Week'] = d_dict['Week'].astype(str).str.zfill(2)
        map_period = dict(zip(d_dict['Week'], d_dict['Period']))
        
        return map_category, map_cluster, map_period
    except Exception as e:
        st.error(f"⚠️ Error cargando diccionarios locales: {e}")
        return None, None, None

map_category, map_cluster, map_period = load_dicts()

# --- 2. MOTOR DE UPSERT (MERGE) PARA BIGQUERY ---
def upsert_to_bigquery(client, df, target_table_id, primary_keys, schema=None):
    """Sube a tabla temporal y ejecuta un MERGE para evitar duplicados en BQ."""
    dataset_id = target_table_id.split('.')[0]
    temp_table_id = f"{dataset_id}.temp_upsert_{uuid.uuid4().hex[:8]}"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True
        
    # 1. Cargar temporal
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result() 
    
    # 2. Armar query MERGE
    match_conditions = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
    cols = [c for c in df.columns]
    update_set = ", ".join([f"t.{col} = s.{col}" for col in cols if col not in primary_keys])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{col}" for col in cols])
    
    query = f"""
    MERGE `{target_table_id}` t
    USING `{temp_table_id}` s
    ON {match_conditions}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    
    client.query(query).result()
    client.query(f"DROP TABLE `{temp_table_id}`").result()

# --- 3. LÓGICAS DE TRANSFORMACIÓN ---
def process_dataframe_A(df, map_category, map_cluster, map_period):
    df = df.iloc[:, :31].copy()
    df.iloc[:, 29] = 0
    df.iloc[:, 30] = 0
    df.iloc[:, 2] = pd.to_datetime(df.iloc[:, 2], errors='coerce').dt.strftime('%Y-%m-%d')
    
    for col_idx in [26, 27, 28]:
        df.iloc[:, col_idx] = df.iloc[:, col_idx].replace({',': ''}, regex=True)
        df.iloc[:, col_idx] = pd.to_numeric(df.iloc[:, col_idx], errors='coerce')
    
    city_col = df.iloc[:, 6].astype(str).str.strip()
    week_col = df.iloc[:, 1].astype(str).str.zfill(2)
    
    df.insert(0, 'Category', city_col.map(map_category).fillna("Other"))
    df.insert(1, 'Cluster', city_col.map(map_cluster).fillna("Other"))
    df.insert(2, 'City Name Map', city_col)
    df.insert(3, 'Period', week_col.map(map_period).fillna("Unknown"))
    return df

def process_dataframe_B(df, map_cluster, map_period):
    subregion_col = df.iloc[:, 0].astype(str).str.strip().str.upper()
    city_name_col = df.iloc[:, 3].astype(str).str.strip()
    
    mask = (subregion_col == 'MX') | (city_name_col.str.contains('Juarez|Juárez|Mazatlan|Mazatlán', case=False, regex=True, na=False))
    df = df[mask].copy().iloc[:, :42]
    df.iloc[:, 32] = 0
    
    city_col = df.iloc[:, 3].astype(str).str.strip() 
    date_col_series = pd.to_datetime(df.iloc[:, 5], errors='coerce') 
    df.iloc[:, 5] = date_col_series.dt.strftime('%Y-%m-%d')
    
    def parse_year_week(val):
        val_str = str(val).strip()
        if '-' in val_str and ':' in val_str:
            try:
                dt = pd.to_datetime(val_str)
                return dt.year, dt.month  
            except:
                return 0, 0
        elif '/' in val_str:
            parts = val_str.split('/')
            try:
                return int(parts[0]), int(parts[1])
            except: return 0, 0
        return 0, 0
        
    parsed_yw = df.iloc[:, 4].apply(parse_year_week)
    year, week = parsed_yw.apply(lambda x: x[0]), parsed_yw.apply(lambda x: x[1])
    
    df['Cluster_AQ'] = city_col.map(map_cluster).fillna("Other")
    df['Weeknum_AR'], df['Weeknum_AS'] = week, week
    df['Year_AT'] = year
    df['Period_AU'] = week.astype(str).str.zfill(2).map(map_period).fillna("Unknown")
    df['DayOfWeek_AV'] = date_col_series.dt.day_name() 
    return df

def process_dataframe_C(df, map_period):
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')
    if 'year_calendar_week' in df.columns:
        week_series = df['year_calendar_week'].astype(str).str.split('/').str[-1]
        df['calendar_week_new'] = pd.to_numeric(week_series, errors='coerce').fillna(0).astype(int)
        df['period'] = df['calendar_week_new'].astype(str).str.zfill(2).map(map_period).fillna("Unknown")
    return df

# --- 4. INTERFAZ Y WORKFLOW ---
if map_category is not None:
    tab1, tab2, tab3 = st.tabs(["📊 Daily Metrics", "📈 Burn SoT", "🎯 Targets ROM"])

    # --- PESTAÑA 1 (Daily Metrics - APPEND) ---
    with tab1:
        with st.container(border=True):
            st.markdown("### 📥 Carga de Daily Metrics")
            st.caption("Esquema esperado: 202406_SSL_MainMetricsWithManaged_di")
            uploaded_files_A = st.file_uploader("Sube el reporte diario (CSV/XLSX)", type=['csv', 'xlsx'], accept_multiple_files=True, key="up_A")

            if uploaded_files_A:
                for file in uploaded_files_A:
                    try:
                        df_A = pd.read_excel(file) if file.name.endswith('.xlsx') else pd.read_csv(file)
                        processed_df_A = process_dataframe_A(df_A, map_category, map_cluster, map_period)
                        
                        st.info(f"✅ Archivo `{file.name}` transformado correctamente.")
                        
                        if st.button(f"🚀 Ejecutar Append hacia BigQuery", key=f"bq_A_{file.name}", type="primary"):
                            with st.spinner("Conectando y subiendo registros..."):
                                creds_dict = st.secrets["gcp_service_account"]
                                client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])
                                
                                archivo_virtual = io.BytesIO(processed_df_A.to_csv(index=False, header=False).encode('utf-8'))
                                job = client.load_table_from_file(archivo_virtual, 'didi_db.Daily DB 100268', job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
                                job.result() 
                            st.success("¡Operación completada en BigQuery! 🎉")
                    except Exception as e: st.error(f"Error procesando {file.name}: {e}")

    # --- PESTAÑA 2 (Burn SoT - APPEND) ---
    with tab2:
        with st.container(border=True):
            st.markdown("### 📥 Carga de Burn SoT")
            st.caption("Esquema esperado: 202508_SoT_Burn_di")
            uploaded_files_B = st.file_uploader("Sube el reporte Burn (CSV/XLSX)", type=['csv', 'xlsx'], accept_multiple_files=True, key="up_B")

            if uploaded_files_B:
                for file in uploaded_files_B:
                    try:
                        df_B = pd.read_excel(file) if file.name.endswith('.xlsx') else pd.read_csv(file)
                        processed_df_B = process_dataframe_B(df_B, map_cluster, map_period)
                        
                        st.info(f"✅ Archivo `{file.name}` transformado correctamente.")

                        if st.button(f"🚀 Ejecutar Append hacia BigQuery", key=f"bq_B_{file.name}", type="primary"):
                            with st.spinner("Parseando esquema y subiendo a BigQuery..."):
                                creds_dict = st.secrets["gcp_service_account"]
                                client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])
                                
                                bq_schema = [
                                    bigquery.SchemaField("subregion", "STRING"), bigquery.SchemaField("country_code", "STRING"), bigquery.SchemaField("city_id", "INTEGER"), bigquery.SchemaField("city_name", "STRING"), bigquery.SchemaField("year_calendar_week", "STRING"), bigquery.SchemaField("date_value", "DATE"), bigquery.SchemaField("product_id", "INTEGER"), bigquery.SchemaField("product_name", "STRING"), bigquery.SchemaField("gmv", "FLOAT"), bigquery.SchemaField("usd_fx", "FLOAT"), bigquery.SchemaField("drv_expan_cx", "FLOAT"), bigquery.SchemaField("drv_expan_mktp", "FLOAT"), bigquery.SchemaField("drv_expan_fleet", "FLOAT"), bigquery.SchemaField("drv_expan_shorterm", "FLOAT"), bigquery.SchemaField("drv_expan_other", "FLOAT"), bigquery.SchemaField("drv_expan_loyalty", "FLOAT"), bigquery.SchemaField("drv_expan_captain", "INTEGER"), bigquery.SchemaField("drv_expan_guru", "FLOAT"), bigquery.SchemaField("drv_react_longterm", "FLOAT"), bigquery.SchemaField("drv_react_fleet", "INTEGER"), bigquery.SchemaField("drv_react_cbcamp", "FLOAT"), bigquery.SchemaField("drv_activ_newgi", "FLOAT"), bigquery.SchemaField("drv_activ_dormant", "FLOAT"), bigquery.SchemaField("drv_activ_fleet", "FLOAT"), bigquery.SchemaField("drv_activ_newcamp", "FLOAT"), bigquery.SchemaField("drv_activ_referral", "FLOAT"), bigquery.SchemaField("drv_activ_paidmkt", "FLOAT"), bigquery.SchemaField("pax_react_longterm", "FLOAT"), bigquery.SchemaField("pax_activ_new", "FLOAT"), bigquery.SchemaField("pax_activ_dormant", "FLOAT"), bigquery.SchemaField("pax_activ_referral", "FLOAT"), bigquery.SchemaField("pax_activ_paid_mkt", "FLOAT"), bigquery.SchemaField("pax_activ_cross", "INTEGER"), bigquery.SchemaField("pax_expan_mktp", "FLOAT"), bigquery.SchemaField("pax_expan_other", "FLOAT"), bigquery.SchemaField("pax_expan_employee", "FLOAT"), bigquery.SchemaField("pax_expan_cx", "FLOAT"), bigquery.SchemaField("pax_expan_didiclub", "FLOAT"), bigquery.SchemaField("pax_expan_shorterm", "FLOAT"), bigquery.SchemaField("pax_expan_riderpass", "FLOAT"), bigquery.SchemaField("pax_expan_surgepass", "FLOAT"), bigquery.SchemaField("drv_expan_driverpass", "FLOAT"), bigquery.SchemaField("CLUSTER", "STRING"), bigquery.SchemaField("WEEKNUM", "INTEGER"), bigquery.SchemaField("calendar_week", "INTEGER"), bigquery.SchemaField("year", "INTEGER"), bigquery.SchemaField("period", "STRING"), bigquery.SchemaField("weekday_name", "STRING")
                                ]

                                processed_df_B.columns = [field.name for field in bq_schema]

                                for field in bq_schema:
                                    col = field.name
                                    if field.field_type == 'INTEGER':
                                        processed_df_B[col] = pd.to_numeric(processed_df_B[col], errors='coerce').round(0).astype('Int64')
                                    elif field.field_type == 'FLOAT':
                                        if processed_df_B[col].dtype == 'object': processed_df_B[col] = processed_df_B[col].astype(str).str.replace(',', '', regex=False)
                                        processed_df_B[col] = pd.to_numeric(processed_df_B[col], errors='coerce').astype('float64')
                                    elif field.field_type == 'STRING':
                                        processed_df_B[col] = processed_df_B[col].astype(str).replace({'nan': '', 'NaN': '', 'None': ''})

                                job = client.load_table_from_dataframe(processed_df_B, 'didi_db.Burn SoT', job_config=bigquery.LoadJobConfig(schema=bq_schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
                                job.result() 
                            st.success("¡Operación completada en BigQuery! 🎉")
                    except Exception as e: st.error(f"Error procesando {file.name}: {e}")

    # --- PESTAÑA 3 (Targets ROM - UPSERT/MERGE) ---
    with tab3:
        with st.container(border=True):
            st.markdown("### 🎯 Carga de Targets ROM")
            st.caption("La ingesta limpiará encabezados e insertará o actualizará metas evitando duplicidades.")
            uploaded_files_C = st.file_uploader("Sube el archivo ROM Planning (CSV)", type=['csv'], accept_multiple_files=True, key="up_C")

            if uploaded_files_C:
                for file in uploaded_files_C:
                    try:
                        df_C = pd.read_csv(file, header=2)
                        processed_df_C = process_dataframe_C(df_C, map_period)
                        
                        st.info(f"✅ Archivo `{file.name}` transformado (Métricas CT/CU calculadas).")

                        if st.button(f"🚀 Ejecutar Inteligente (Upsert) hacia BigQuery", key=f"bq_C_{file.name}", type="primary"):
                            with st.spinner("Sincronizando Targets y eliminando obsoletos..."):
                                creds_dict = st.secrets["gcp_service_account"]
                                client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])
                                
                                # PKs solicitadas para el Merge
                                primary_keys_c = ['city_name', 'year_calendar_week', 'year']
                                upsert_to_bigquery(client, processed_df_C, 'didi_db.Targets_ROM', primary_keys_c)
                                
                            st.success("¡Targets actualizados con éxito sin duplicados! 🎉")
                    except Exception as e: st.error(f"Error procesando {file.name}: {e}")

# --- 5. PANEL DE MANTENIMIENTO AVANZADO ---
with st.expander("🛠️ Herramientas de Base de Datos (Mantenimiento Daily DB)"):
    st.markdown("Usa estos controles en caso de fallos, registros repetidos o ingestas dobles accidentales.")
    col_clean, col_delete = st.columns(2)
    
    with col_clean:
        st.markdown("#### 🧹 Deduplicación Automática")
        st.caption("Corre un escaneo de 7 días atrás, borrando filas repetidas pero manteniendo registros únicos.")
        if st.button("Ejecutar Deduplicación (T-7 a T-1)", use_container_width=True):
            with st.spinner("Ejecutando Scripts de limpieza en BigQuery..."):
                try:
                    creds_dict = st.secrets["gcp_service_account"]
                    client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])
                    
                    hoy = datetime.date.today()
                    for i in range(1, 8):
                        fecha = (hoy - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                        query = f"""
                        CREATE TEMP TABLE datos_limpios AS SELECT DISTINCT * FROM `didi_db.Daily DB 100268` WHERE date_value = '{fecha}';
                        DELETE FROM `didi_db.Daily DB 100268` WHERE date_value = '{fecha}';
                        INSERT INTO `didi_db.Daily DB 100268` SELECT * FROM datos_limpios;
                        """
                        client.query(query).result()
                    st.success("¡Limpieza de los últimos 7 días completada satisfactoriamente!")
                except Exception as e: st.error(f"Fallo en consulta de limpieza: {e}")

    with col_delete:
        st.markdown("#### 🗑️ Purga Manual por Fecha")
        st.caption("Elimina el 100% de la data del día seleccionado si necesitas re-ingestar todo manualmente.")
        fecha_borrado = st.date_input("Fecha a purgar", value=datetime.date.today() - datetime.timedelta(days=1))
        
        if st.button("Purgar Data del Día", type="primary", use_container_width=True):
            with st.spinner(f"Vaciando partición del {fecha_borrado}..."):
                try:
                    creds_dict = st.secrets["gcp_service_account"]
                    client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])
                    
                    query = f"DELETE FROM `didi_db.Daily DB 100268` WHERE date_value = '{fecha_borrado.strftime('%Y-%m-%d')}';"
                    client.query(query).result()
                    st.success(f"Partición del {fecha_borrado} vaciada con éxito.")
                except Exception as e: st.error(f"Fallo ejecutando purga: {e}")
