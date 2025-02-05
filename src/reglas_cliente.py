import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.oauth2 import service_account
from tkinter import Tk, messagebox
import logging
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def procesar_reglas_cliente():
    # -----------------------------------------------------------
    # 1. Definición de tablas y rutas de credenciales
    # -----------------------------------------------------------
    # Tablas de BigQuery
    table_main = 'bx-opeopt-prod.opeopt_tbl_parametricas_dtl.registros_reglas'
    table_details = 'bx-opeopt-prod.opeopt_tbl_parametricas_dtl.detalle_reglas'
    table_carga_masiva = 'bx-opeopt-prod.opeopt_tbl_parametricas_dtl.carga_masiva_reglas'

    # Tablas de PostgreSQL (texto en fechas)
    schema_name = 'fullfill'
    staging_table_name = 'clientes_reglas_dev1'       # Tabla staging
    prod_table_name = 'clientes_reglas_dev'           # Tabla principal
    backup_table_name = 'clientes_reglas_respaldo'    # Tabla de respaldo

    # Credenciales de BigQuery
    ruta_credenciales = r'C:\workspace-blue\blue-attached\automatizacion\utils\GOOGLE_APPLICATION_CREDENTIALS.JSON'

    # Función auxiliar para ordenar cadenas de ID_regla de forma "natural"
    def sort_key(value):
        try:
            return (0, int(value))
        except (ValueError, TypeError):
            return (1, str(value))

    root = Tk()
    root.withdraw()

    try:
        # -----------------------------------------------------------
        # 2. Conexión a BigQuery y lectura de datos
        # -----------------------------------------------------------
        logging.info("Configurando credenciales de Google Cloud.")
        credentials = service_account.Credentials.from_service_account_file(ruta_credenciales)
        client = bigquery.Client(credentials=credentials)
        bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

        logging.info("Ejecutando consultas en BigQuery.")
        query_main = f"SELECT * FROM `{table_main}`"
        query_details = f"SELECT * FROM `{table_details}`"
        query_carga_masiva = f"SELECT * FROM `{table_carga_masiva}`"

        main_data_df = client.query(query_main).result().to_dataframe(bqstorage_client=bqstorageclient)
        detalles_data_df = client.query(query_details).result().to_dataframe(bqstorage_client=bqstorageclient)
        carga_masiva_data_df = client.query(query_carga_masiva).result().to_dataframe(bqstorage_client=bqstorageclient)

        # -----------------------------------------------------------
        # 3. Limpieza y normalización de DataFrames
        # -----------------------------------------------------------
        logging.info("Procesando datos con pandas.")
        # a) Nombres de columnas en minúsculas
        main_data_df.columns = main_data_df.columns.str.strip().str.lower()
        detalles_data_df.columns = detalles_data_df.columns.str.strip().str.lower()
        carga_masiva_data_df.columns = carga_masiva_data_df.columns.str.strip().str.lower()

        # b) Convertir llaves a string
        if 'id_registro' in main_data_df.columns:
            main_data_df['id_registro'] = main_data_df['id_registro'].astype('string')
        if 'id_registro_ref' in detalles_data_df.columns:
            detalles_data_df['id_registro_ref'] = detalles_data_df['id_registro_ref'].astype('string')

        # c) Renombrar 'fecha_termino' a 'fecha_fin' si aparece
        for df in [main_data_df, detalles_data_df, carga_masiva_data_df]:
            if 'fecha_termino' in df.columns:
                df.rename(columns={'fecha_termino': 'fecha_fin'}, inplace=True)

        # d) Asegurarnos de que 'fecha_inicio' y 'fecha_fin' sean string, 
        #    y poner None donde estén vacíos
        for df in [main_data_df, carga_masiva_data_df]:
            for col in ['fecha_inicio', 'fecha_fin']:
                if col in df.columns:
                    df[col] = df[col].astype('string')
                    # Reemplazar manualmente, para no usar .replace(...) (que a veces da error en Pandas)
                    df.loc[df[col] == '',     col] = None
                    df.loc[df[col] == 'nan',  col] = None
                    df.loc[df[col] == 'NaN',  col] = None
                    df.loc[df[col] == 'None', col] = None

        # -----------------------------------------------------------
        # 4. Merge entre main y detalles
        # -----------------------------------------------------------
        merged_df = pd.merge(
            main_data_df,
            detalles_data_df,
            left_on='id_registro',
            right_on='id_registro_ref',
            how='left',
            indicator=True
        )

        # Agrupamos las reglas por (cliente, fecha_inicio, fecha_fin)
        resultado = (
            merged_df
            .groupby(['cliente', 'fecha_inicio', 'fecha_fin'], dropna=False)['id_regla']
            .apply(lambda x: ','.join(sorted(x.dropna().astype(str), key=sort_key)))
            .reset_index()
        )
        resultado.rename(columns={'id_regla': 'reglas'}, inplace=True)
        resultado['reglas'] = resultado['reglas'].fillna('')

        # -----------------------------------------------------------
        # 5. Agrupar la info de carga masiva
        # -----------------------------------------------------------
        if 'cliente' not in carga_masiva_data_df.columns:
            carga_masiva_data_df['cliente'] = 'SIN_CLIENTE'
        else:
            carga_masiva_data_df['cliente'] = carga_masiva_data_df['cliente'].fillna('SIN_CLIENTE')

        carga_masiva_resultado = (
            carga_masiva_data_df
            .groupby(['cliente', 'fecha_inicio', 'fecha_fin'], dropna=False)['id_regla']
            .apply(lambda x: ','.join(sorted(x.dropna().astype(str), key=sort_key)))
            .reset_index()
        )
        carga_masiva_resultado.rename(columns={'id_regla': 'reglas'}, inplace=True)

        # -----------------------------------------------------------
        # 6. Unir resultado + carga_masiva_resultado
        # -----------------------------------------------------------
        union_df = pd.concat([resultado, carga_masiva_resultado], ignore_index=True)

        final_result = (
            union_df
            .groupby(['cliente', 'fecha_inicio', 'fecha_fin'], dropna=False)['reglas']
            .apply(lambda series_reglas:
                   ','.join(
                       sorted(
                           set(",".join(series_reglas).split(',')),
                           key=sort_key
                       )
                   )
                  )
            .reset_index()
        )

        # -----------------------------------------------------------
        # 7. Limpiar final_result de 'nan', etc. y asegurar texto
        # -----------------------------------------------------------
        for col in ['cliente', 'fecha_inicio', 'fecha_fin']:
            if col in final_result.columns:
                final_result[col] = final_result[col].astype('string')
                final_result.loc[final_result[col] == 'nan',   col] = None
                final_result.loc[final_result[col] == 'NaN',   col] = None
                final_result.loc[final_result[col] == 'None',  col] = None
                final_result.loc[final_result[col] == '',      col] = None

        if 'reglas' in final_result.columns:
            final_result['reglas'] = final_result['reglas'].astype('string').fillna('')

        # -----------------------------------------------------------
        # 8. Conexión a PostgreSQL
        # -----------------------------------------------------------
        logging.info("Preparando conexión a PostgreSQL.")
        db_username = 'bx_fullfill_ejfull_etl_wr'
        db_password = 'bxfulejf2024*'
        db_host = '172.16.7.109'
        db_port = '5432'
        db_name = 'dwh'

        db_password_encoded = quote_plus(db_password)
        connection_string = f'postgresql+psycopg2://{db_username}:{db_password_encoded}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(connection_string)

        # -----------------------------------------------------------
        # A) STAGING (clientes_reglas_dev1): TRUNCATE + Insert
        # -----------------------------------------------------------
        logging.info(f"Vaciando la tabla staging {staging_table_name} (TRUNCATE).")
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"TRUNCATE TABLE {schema_name}.{staging_table_name};")
            )

        from sqlalchemy import types
        logging.info(f"Insertando datos en tabla staging {staging_table_name}.")
        with engine.begin() as conn:
            final_result.to_sql(
                name=staging_table_name,
                con=conn,
                schema=schema_name,
                if_exists='append',
                index=False,
                dtype={
                    'cliente': types.String(),
                    'fecha_inicio': types.String(),
                    'fecha_fin': types.String(),
                    'reglas': types.String()
                }
            )

        # Eliminar duplicados y registros sin reglas en STAGING
        with engine.begin() as conn:
            logging.info(f"Eliminando filas duplicadas en staging {staging_table_name}.")
            delete_duplicates_query = text(f"""
                WITH cte AS (
                    SELECT 
                        ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY cliente, fecha_inicio, fecha_fin 
                            ORDER BY cliente
                        ) AS rn
                    FROM {schema_name}.{staging_table_name}
                )
                DELETE FROM {schema_name}.{staging_table_name}
                WHERE ctid IN (
                    SELECT ctid FROM cte WHERE rn > 1
                );
            """)
            conn.execute(delete_duplicates_query)

            logging.info(f"Eliminando registros sin reglas en staging {staging_table_name}.")
            delete_query = text(f"DELETE FROM {schema_name}.{staging_table_name} WHERE reglas = '';")
            conn.execute(delete_query)

        # -----------------------------------------------------------
        # B) Upsert manual en la tabla PRINCIPAL (clientes_reglas_dev)
        # -----------------------------------------------------------
        with engine.begin() as conn:
            logging.info(f"Actualizando filas que ya existen en {prod_table_name}.")
            update_query = text(f"""
                UPDATE {schema_name}.{prod_table_name} p
                SET reglas = s.reglas
                FROM {schema_name}.{staging_table_name} s
                WHERE p.cliente      = s.cliente
                  AND p.fecha_inicio = s.fecha_inicio
                  AND p.fecha_fin    = s.fecha_fin
            """)
            conn.execute(update_query)

        with engine.begin() as conn:
            logging.info(f"Insertando filas que no existen en {prod_table_name}.")
            insert_query = text(f"""
                INSERT INTO {schema_name}.{prod_table_name} (cliente, reglas, fecha_inicio, fecha_fin)
                SELECT s.cliente, s.reglas, s.fecha_inicio, s.fecha_fin
                FROM {schema_name}.{staging_table_name} s
                LEFT JOIN {schema_name}.{prod_table_name} p
                  ON p.cliente      = s.cliente
                 AND p.fecha_inicio = s.fecha_inicio
                 AND p.fecha_fin    = s.fecha_fin
                WHERE p.cliente IS NULL
            """)
            conn.execute(insert_query)

        # (Opcional) Eliminar duplicados en la tabla principal
        with engine.begin() as conn:
            logging.info(f"Eliminando duplicados en la tabla principal {prod_table_name}.")
            remove_dupes_prod_query = text(f"""
                WITH cte AS (
                    SELECT
                        ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY cliente, fecha_inicio, fecha_fin
                            ORDER BY cliente
                        ) AS rn
                    FROM {schema_name}.{prod_table_name}
                )
                DELETE FROM {schema_name}.{prod_table_name}
                WHERE ctid IN (
                    SELECT ctid
                    FROM cte
                    WHERE rn > 1
                );
            """)
            conn.execute(remove_dupes_prod_query)

        # (Opcional) Eliminar registros sin reglas
        with engine.begin() as conn:
            logging.info(f"Eliminando registros sin reglas en {prod_table_name}.")
            delete_prod_query = text(f"DELETE FROM {schema_name}.{prod_table_name} WHERE reglas = '';")
            conn.execute(delete_prod_query)

        # -----------------------------------------------------------
        # C) Respaldo: tomar foto de la tabla STAGING (clientes_reglas_dev1)
        # -----------------------------------------------------------
        with engine.begin() as conn:
            logging.info(f"Generando respaldo desde la tabla staging {staging_table_name} hacia {backup_table_name}.")
            backup_query = text(f"""
                INSERT INTO {schema_name}.{backup_table_name} 
                    (cliente, reglas, fecha_inicio, fecha_fin, fecha_respaldo)
                SELECT
                    s.cliente,
                    s.reglas,
                    s.fecha_inicio,
                    s.fecha_fin,
                    NOW() AS fecha_respaldo
                FROM {schema_name}.{staging_table_name} s
            """)
            conn.execute(backup_query)

        logging.info("Proceso completado exitosamente.")
        messagebox.showinfo("Notificación", "La operación se completó exitosamente.")

    except IntegrityError as e:
        logging.error(f"Error de integridad en la base de datos: {e.orig}")
        messagebox.showerror("Error de Integridad", f"Error de integridad en la base de datos: {e.orig}")
    except Exception as e:
        logging.exception("Ocurrió un error inesperado:")
        messagebox.showerror("Error", f"Ocurrió un error inesperado: {e}")
    finally:
        root.destroy()

#if __name__ == "__main__":
    #procesar_reglas_cliente()
