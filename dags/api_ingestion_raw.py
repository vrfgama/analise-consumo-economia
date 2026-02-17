from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import logging
from pathlib import Path
import psycopg2
import os
import time

from bcb_endpoints import BCB_ENDPOINTS
from ibge_endpoints import IBGE_ENDPOINTS



def extract_api_raw(**context):

    dag_id = context["dag"].dag_id
    execution_date = context["data_interval_start"]

    request_start_time = datetime.utcnow()
    start_ts = time.time()

    ENDPOINTS = BCB_ENDPOINTS + IBGE_ENDPOINTS

    # conexão com o banco (1 por task)
    conn = psycopg2.connect(
        host="postgres",
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        port=5432
    )
    cursor = conn.cursor()

    for endpoint in ENDPOINTS:

        status = "SUCCESS"
        error_message = None
        error_type = None
        http_status_code = None
        records_count = None
        response_size_kb = None
        is_empty_response = None
        file_name = None
        file_path = None

        api_name = endpoint["api"]
        indicator_name = endpoint["name"]
        api_url = endpoint["url"]
        params = endpoint.get("params")
        http_method = "GET"
        request_params = params

        try:
            response = requests.get(api_url, params=params, timeout=30)
            http_status_code = response.status_code
            response.raise_for_status()

            data = response.json()

            # =========================
            # Armazenamento RAW
            # =========================
            date_str = execution_date.strftime("%Y-%m-%d")
            time_str = execution_date.strftime("%H%M%S")

            raw_dir = Path(
                f"/opt/airflow/data/raw/{api_name}/{indicator_name}/{date_str}"
            )
            raw_dir.mkdir(parents=True, exist_ok=True)

            file_name = f"{api_name}_{indicator_name}_{time_str}.json"
            file_path = raw_dir / file_name

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # =========================
            # Métricas
            # =========================
            response_size_kb = round(file_path.stat().st_size / 1024, 2)

            if isinstance(data, list):
                records_count = len(data)
                is_empty_response = len(data) == 0
            elif isinstance(data, dict):
                records_count = len(data)
                is_empty_response = len(data) == 0
            else:
                records_count = None
                is_empty_response = None

        except Exception as e:
            status = "FAILED"
            error_message = str(e)
            error_type = type(e).__name__
            logging.error(f"[{api_name} - {indicator_name}] {error_message}")

        request_end_time = datetime.utcnow()
        duration_ms = int((time.time() - start_ts) * 1000)

        # =========================
        # Inserção do LOG
        # =========================
        insert_sql = """
        INSERT INTO logs.api_ingestion_log (
            dag_id,
            execution_date,
            api_name,
            api_url,
            http_method,
            request_params,
            request_start_time,
            request_end_time,
            duration_ms,
            status,
            http_status_code,
            records_count,
            response_size_kb,
            is_empty_response,
            file_name,
            file_path,
            file_format,
            error_message,
            error_type,
            retry_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_sql,
            (
                dag_id,
                execution_date,
                f"{api_name}.{indicator_name}",
                api_url,
                http_method,
                json.dumps(request_params) if request_params else None,
                request_start_time,
                request_end_time,
                duration_ms,
                status,
                http_status_code,
                records_count,
                response_size_kb,
                is_empty_response,
                file_name if status == "SUCCESS" else None,
                str(file_path) if status == "SUCCESS" else None,
                "json",
                error_message,
                error_type,
                0  # retry_count
            )
        )

        conn.commit()

    cursor.close()
    conn.close()

    logging.info("Ingestão RAW finalizada com sucesso.")


import os
import json
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


def process_raw_indicator_files():

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    base_path = "/opt/airflow/data/raw"

    logging.info("Iniciando varredura de arquivos raw...")

    for root, dirs, files in os.walk(base_path):
        for file in files:

            if not file.endswith(".json"):
                continue

            file_path = os.path.join(root, file)
            logging.info(f"Processando arquivo: {file_path}")

            # -------------------------------------------------
            # 🔎 Verifica se existe log com success = true
            # -------------------------------------------------
            cursor.execute("""
                SELECT 1
                FROM logs.api_ingestion_log
                WHERE file_name = %s
                  AND status = 'SUCCESS'
                LIMIT 1
            """, (file,))

            log_ok = cursor.fetchone()

            if not log_ok:
                logging.info(f"Arquivo {file} ignorado (sem success no log).")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # ==========================================================
            # IBGE (estrutura com "resultados")
            # ==========================================================

            if isinstance(data, list) and len(data) > 0 and "resultados" in data[0]:

                parts = file.replace(".json", "").split("_")

                aggregate_code = None
                variable_code = None

                for part in parts:
                    if part.startswith("ag") and part[2:].isdigit():
                        aggregate_code = part[2:]
                    if part.startswith("var") and part[3:].isdigit():
                        variable_code = part[3:]

                if not aggregate_code or not variable_code:
                    logging.warning(f"Códigos IBGE inválidos no arquivo {file}")
                    continue

                resultados = data[0]["resultados"]

                # ======================================================
                # 🔵 CASO 3418 (COM CATEGORY)
                # ======================================================
                if aggregate_code == "3418":

                    for resultado in resultados:

                        category_code = None

                        for classificacao in resultado.get("classificacoes", []):
                            categoria = classificacao.get("categoria", {})
                            if isinstance(categoria, dict) and categoria:
                                category_code = list(categoria.keys())[0]

                        if not category_code:
                            continue

                        cursor.execute("""
                            SELECT id
                            FROM staging.d_indicator_metadata
                            WHERE aggregate_code = %s
                            AND variable_code = %s
                            AND category_code = %s
                        """, (int(aggregate_code), int(variable_code), int(category_code)))

                        result = cursor.fetchone()
                        if not result:
                            continue

                        indicator_id = result[0]

                        for serie in resultado["series"]:
                            for ref, val in serie["serie"].items():

                                if val == "-" or val is None:
                                    continue

                                reference_date = datetime.strptime(ref, "%Y%m").date()
                                value = float(val.replace(",", "."))

                                cursor.execute("""
                                    INSERT INTO staging.f_indicator_value
                                    (indicator_id, reference_date, value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (indicator_id, reference_date)
                                    DO UPDATE SET value = EXCLUDED.value
                                """, (indicator_id, reference_date, value))

                # ======================================================
                # 🟢 OUTROS IBGE (3416, 6381 etc.)
                # ======================================================
                else:

                    cursor.execute("""
                        SELECT id
                        FROM staging.d_indicator_metadata
                        WHERE aggregate_code = %s
                        AND variable_code = %s
                        AND category_code IS NULL
                    """, (int(aggregate_code), int(variable_code)))

                    result = cursor.fetchone()
                    if not result:
                        continue

                    indicator_id = result[0]

                    for resultado in resultados:
                        for serie in resultado["series"]:
                            for ref, val in serie["serie"].items():

                                if val == "-" or val is None:
                                    continue

                                reference_date = datetime.strptime(ref, "%Y%m").date()
                                value = float(val.replace(",", "."))

                                cursor.execute("""
                                    INSERT INTO staging.f_indicator_value
                                    (indicator_id, reference_date, value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (indicator_id, reference_date)
                                    DO UPDATE SET value = EXCLUDED.value
                                """, (indicator_id, reference_date, value))




            # ==========================================================
            # BCB
            # ==========================================================

            elif "sgs" in file:

                variable_code = None
                parts = file.split("_")

                for part in parts:
                    if part.startswith("sgs"):
                        variable_code = part.replace("sgs", "")
                        break

                if not variable_code:
                    logging.warning(f"Variable_code BCB não encontrado em {file}")
                    continue

                cursor.execute("""
                    SELECT id
                    FROM staging.d_indicator_metadata
                    WHERE variable_code = %s
                """, (variable_code,))

                result = cursor.fetchone()

                if not result:
                    logging.warning(f"Metadata não encontrado BCB {variable_code}")
                    continue

                indicator_id = result[0]

                for row in data:

                    reference_date = datetime.strptime(row["data"], "%d/%m/%Y").date()
                    value = float(row["valor"].replace(",", "."))

                    cursor.execute("""
                        INSERT INTO staging.f_indicator_value
                        (indicator_id, reference_date, value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (indicator_id, reference_date)
                        DO UPDATE SET value = EXCLUDED.value
                    """, (indicator_id, reference_date, value))

    # -------------------------------------------------
    # Commit fora do loop (IMPORTANTE)
    # -------------------------------------------------
    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Processamento finalizado com sucesso.")



with DAG(
    dag_id="api_ingestion_raw",
    start_date=datetime(2026, 1, 22),
    schedule_interval=None,
    catchup=False,
    tags=["api", "raw"]
) as dag:

    extract_raw = PythonOperator(
        task_id="extract_api_raw",
        python_callable=extract_api_raw
    )

    process_task = PythonOperator(
    task_id="process_raw_indicator_files",
    python_callable=process_raw_indicator_files
)

extract_raw >> process_task