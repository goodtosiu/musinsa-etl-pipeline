import os
import json
import requests
import re
from datetime import datetime, timedelta
from itertools import islice

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from tenacity import retry, stop_after_attempt, wait_fixed

# ---------------------------------------------------------
# 설정 및 상수 정의
# ---------------------------------------------------------
BATCH_SIZE = 1000
ID_FILE_PATH = '/opt/airflow/dags/data/item_ids.txt'
MYSQL_CONN_ID = 'my_mysql_conn'
GPU_API_URL = "http://<GPU_서버_IP>:8000/embed"  # IP 확인하고 변경합시다.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0, 
}

# ---------------------------------------------------------
# 텍스트 불용어 정제 함수
# ---------------------------------------------------------
def clean_goods_name(text):
    if not text:
        return ""
    # 1. 대괄호 및 그 안의 내용 제거 (예: [스토커즈 콜라보])
    text = re.sub(r'\[.*?\]', '', text)
    # 2. 'Ncolor', 'N종' 등의 옵션 키워드 제거 (예: 10color, 4color)
    text = re.sub(r'\d+\s*(color|종|가지색상)', '', text, flags=re.IGNORECASE)
    # 3. 특수문자 제거 및 다중 공백을 단일 공백으로 압축
    text = re.sub(r'[^a-zA-Z0-9가-힣\s]', ' ', text)
    return ' '.join(text.split())

# ---------------------------------------------------------
# API 재시도 로직 (Tenacity)
# ---------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_api_with_retry(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers, timeout=5)
    
    if response.status_code == 404:
        return None, 404
        
    response.raise_for_status() 
    return response.json(), 200

# ---------------------------------------------------------
# E-T-L 메인 파이썬 로직
# ---------------------------------------------------------
def run_etl_batch(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    # 1. 상태 확인 (pipeline_state)
    cursor.execute("SELECT last_processed_index FROM pipeline_state WHERE job_name='clothing_etl'")
    result = cursor.fetchone()
    if not result:
        raise Exception("pipeline_state 테이블에 'clothing_etl' 레코드가 없습니다.")
    last_index = result[0]
    
    # 2. 텍스트 파일에서 ID 1000개 추출 (Extract)
    if not os.path.exists(ID_FILE_PATH):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {ID_FILE_PATH}")
        
    with open(ID_FILE_PATH, 'r') as f:
        generator = islice(f, last_index, last_index + BATCH_SIZE)
        batch_ids = [line.strip() for line in generator if line.strip()]
        
    if not batch_ids:
        cursor.close()
        conn.close()
        return 'end_pipeline'
        
    raw_data_list = []
    error_log_list = []
    valid_items_temp = []
    transformed_data_list = []
    
    # 3. 1,000개 ID 순회 및 API 요청/가공 (Extract & Transform)
    for item_id in batch_ids:
        try:
            size_url = f"https://goods-detail.musinsa.com/api2/goods/{item_id}/actual-size"
            curation_url = f"https://goods-detail.musinsa.com/api2/goods/{item_id}/curation"
            
            size_data, size_status = fetch_api_with_retry(size_url)
            curation_data, curation_status = fetch_api_with_retry(curation_url)
            
            # 404 에러 시 스킵
            if size_status == 404 or curation_status == 404:
                error_log_list.append((item_id, '404 Not Found', 'Size or Curation data missing'))
                continue
                
            # [L: 원천 데이터] 병합 후 임시 저장
            combined_raw = {"actual_size": size_data, "curation": curation_data}
            raw_data_list.append((item_id, json.dumps(combined_raw, ensure_ascii=False)))
            
            # [T: 데이터 파싱 및 정제]
            type_name = size_data.get("data", {}).get("typeName", "")
            goods_name = ""
            brand_name = ""
            
            tabs = curation_data.get("data", {}).get("curationTabs", [])
            for tab in tabs:
                for goods in tab.get("curationGoodsList", []):
                    if str(goods.get("goodsNo")) == str(item_id):
                        goods_name = goods.get("goodsName", "")
                        brand_name = goods.get("brandName", "")
                        break
                if goods_name:
                    break
                    
            cleaned_goods_name = clean_goods_name(goods_name)
            combined_text = f"{brand_name} {type_name} {cleaned_goods_name}".strip()
            
            valid_items_temp.append({
                "item_id": item_id,
                "brand_name": brand_name,
                "type_name": type_name,
                "goods_name": cleaned_goods_name,
                "combined_text": combined_text
            })
            
        except Exception as e:
            raise Exception(f"Failed to process item {item_id}: {str(e)}")

    # 4. GPU 서버로 일괄 임베딩 요청 및 데이터 취합 (Batch Processing)
    if valid_items_temp:
        texts_to_embed = [item["combined_text"] for item in valid_items_temp]
        
        try:
            response = requests.post(GPU_API_URL, json={"texts": texts_to_embed}, timeout=60)
            response.raise_for_status()
            embeddings = response.json().get("embeddings", [])
            
            for item, emb_vector in zip(valid_items_temp, embeddings):
                transformed_data_list.append((
                    item["item_id"],
                    item["brand_name"],
                    item["type_name"],
                    item["goods_name"],
                    item["combined_text"],
                    json.dumps(emb_vector)
                ))
        except Exception as e:
            raise Exception(f"GPU Embedding Server Error: {str(e)}")

    # 5. MySQL 데이터 Bulk Insert (Load)
    if error_log_list:
        cursor.executemany(
            "INSERT INTO error_log (item_id, error_type, error_message) VALUES (%s, %s, %s)", 
            error_log_list
        )
        
    if raw_data_list:
        cursor.executemany(
            "INSERT IGNORE INTO raw_clothing_data (item_id, raw_json) VALUES (%s, %s)", 
            raw_data_list
        )
        
    if transformed_data_list:
        cursor.executemany(
            """INSERT IGNORE INTO transformed_clothing_data 
               (item_id, brand_name, type_name, goods_name, combined_text, embedding) 
               VALUES (%s, %s, %s, %s, %s, %s)""", 
            transformed_data_list
        )
        
    # 6. 상태 업데이트 (pipeline_state)
    new_index = last_index + len(batch_ids)
    cursor.execute("UPDATE pipeline_state SET last_processed_index = %s WHERE job_name='clothing_etl'", (new_index,))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return 'trigger_next_batch'

# ---------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------
with DAG(
    'clothing_etl_pipeline',
    default_args=default_args,
    description='1000개 배치 ELT 파이프라인 (GPU 외부 API 연동 및 재귀 트리거)',
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1 
) as dag:

    etl_task = BranchPythonOperator(
        task_id='run_etl_batch',
        python_callable=run_etl_batch,
        provide_context=True,
    )
    
    trigger_next = TriggerDagRunOperator(
        task_id='trigger_next_batch',
        trigger_dag_id='clothing_etl_pipeline',
        reset_dag_run=False,
    )
    
    end_task = EmptyOperator(
        task_id='end_pipeline'
    )
    
    etl_task >> [trigger_next, end_task]