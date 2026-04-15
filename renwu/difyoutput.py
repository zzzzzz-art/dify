# -*- encoding: utf-8 -*-
"""
脚本名称: dify_workflow_invoke.py
功能: 从 Hive 源表读取搜索日志数据，调用 Dify 工作流 API 进行模型排序评测，
     并将结果写入 Hive 目标表。
依赖: pyspark, requests, pandas, argparse
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------- 日志配置 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------- Dify API 调用函数 ----------
def call_dify_workflow(
    api_key: str,
    workflow_id: str,
    dify_url: str,
    inputs: Dict[str, str],
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Optional[Dict]:
    """
    调用 Dify 工作流 API（同步执行模式）。

    Args:
        api_key: Dify API 密钥
        workflow_id: 工作流 ID（放在请求体中）
        dify_url: Dify API 基础地址（如 https://difyapi.17usoft.com/v1）
        inputs: 工作流输入参数（dict）
        max_retries: 最大重试次数
        retry_delay: 初始重试间隔（秒）

    Returns:
        成功时返回 API 响应中的 outputs 部分（dict）；失败返回 None
    """
    url = f"{dify_url.rstrip('/')}/workflows/run"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "workflow_id": workflow_id,
        "inputs": inputs,
        "response_mode": "blocking",
        "user": "spark_task",
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                outputs = data.get("data", {}).get("outputs", {})
                if outputs:
                    return outputs
                else:
                    logger.warning("API 返回 success 但 outputs 为空: %s", data)
                    return None
            else:
                logger.warning(
                    "HTTP %s, attempt %d/%d, response: %s",
                    resp.status_code,
                    attempt + 1,
                    max_retries,
                    resp.text[:200],
                )
        except Exception as e:
            logger.warning("请求异常 attempt %d/%d: %s", attempt + 1, max_retries, str(e))

        if attempt < max_retries - 1:
            time.sleep(retry_delay * (2**attempt))

    logger.error("调用 Dify 工作流最终失败，inputs: %s", inputs)
    return None


# ---------- 解析 Dify 输出 ----------
def parse_dify_output(outputs: Dict) -> tuple:
    """
    解析 Dify 返回的 outputs，提取评分字段。

    Args:
        outputs: API 返回的 outputs 字典

    Returns:
        (dify_score, core_position, chaos, special_penalty)
    """
    try:
        # outputs 结构: {"result": [{"result": "{\"评分\":..., ...}"}]}
        result_list = outputs.get("result", [])
        if isinstance(result_list, list) and len(result_list) > 0:
            result_str = result_list[0].get("result", "{}")
            final_result = json.loads(result_str)
            score = final_result.get("评分")
            core_pos = final_result.get("核心结果位置")
            chaos = final_result.get("混乱度")
            special_penalty = final_result.get("特殊场景扣分")
            # 可选：评分依据不保存到表，仅记录日志
            reason = final_result.get("评分依据", "")
            if reason:
                logger.debug("评分依据: %s", reason[:200])
            return score, core_pos, chaos, special_penalty
        else:
            logger.warning("outputs 中没有 result 列表: %s", outputs)
            return None, None, None, None
    except Exception as e:
        logger.error("解析 outputs 失败: %s, outputs: %s", e, outputs)
        return None, None, None, None


# ---------- 分区处理函数（用于 foreachPartition） ----------
def process_partition(
    partition_rows,
    api_key: str,
    workflow_id: str,
    dify_url: str,
    target_year: int,
    target_month: int,
    target_day: int,
    result_table: str,
):
    """
    在每个分区内逐行调用 Dify API，收集结果并写入 Hive 表。
    注意：此函数运行在 Executor 上，不能直接使用 SparkSession 的 write，
         因此先将结果收集到列表，最后通过新 SparkSession 写入。
         但为了性能，建议使用 foreachBatch 或 collect 后 Driver 写入。
         此处采用每个分区内写入（需注意并发写同分区问题）。
         简化：我们将在 Driver 端统一写入，因此本函数只返回结果列表。
    """
    import pandas as pd
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.error("无法获取 SparkSession")
        return []

    results = []
    for row in partition_rows:
        inputs = {
            "keyword": row.keyword,
            "cityid": str(row.cityid),
            "userlat": str(row.userlat),
            "userlon": str(row.userlon),
            "pgpath": row.pgpath,
            "caller": row.caller,
        }
        outputs = call_dify_workflow(api_key, workflow_id, dify_url, inputs)

        if outputs:
            score, core_pos, chaos, special_penalty = parse_dify_output(outputs)
        else:
            score = core_pos = chaos = special_penalty = None

        results.append({
            "traceid": row.traceid,
            "keyword": row.keyword,
            "cityid": row.cityid,
            "userlat": row.userlat,
            "userlon": row.userlon,
            "pgpath": row.pgpath,
            "caller": row.caller,
            "dify_score": score,
            "core_position": core_pos,
            "chaos": chaos,
            "special_penalty": special_penalty,
            "year": target_year,
            "month": target_month,
            "day": target_day,
        })

    return results


# ---------- 主函数 ----------
def main():
    parser = argparse.ArgumentParser(description="从 Hive 读取搜索日志，调用 Dify 工作流，写入结果表")
    parser.add_argument("--source-table", type=str, required=True, help="源 Hive 表名")
    parser.add_argument("--target-table", type=str, required=True, help="目标 Hive 表名")
    parser.add_argument("--dt", type=str, required=True, help="源表分区日期，格式 yyyyMMdd")
    parser.add_argument("--dify-workflow-id", type=str, required=True, help="Dify 工作流 ID")
    parser.add_argument("--dify-url", type=str, default="https://difyapi.17usoft.com/v1", help="Dify API 基础地址")
    parser.add_argument("--dify-api-key", type=str, required=True, help="Dify API Key")
    parser.add_argument("--batch-size", type=int, default=100, help="每个 partition 内批量大小（控制内存，本脚本未使用）")
    args = parser.parse_args()

    # 解析分区日期
    dt_obj = datetime.strptime(args.dt, "%Y%m%d")
    year, month, day = dt_obj.year, dt_obj.month, dt_obj.day

    # 创建 SparkSession
    spark = (
        SparkSession.builder.appName("DifyWorkflowInvoke")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # 读取源表
    sql = f"""
        SELECT
            traceid,
            keyword,
            cityid,
            userlat,
            userlon,
            pgpath,
            caller
        FROM {args.source_table}
        WHERE dt = '{args.dt}'
        AND traceid IS NOT NULL
        AND keyword IS NOT NULL
    """
    df = spark.sql(sql)

    # 按 traceid 去重（每个 traceid 只保留一行，假设输入字段相同）
    df = df.dropDuplicates(["traceid"])

    total = df.count()
    if total == 0:
        logger.info("源表分区 %s 没有数据，任务结束。", args.dt)
        spark.stop()
        return

    logger.info("共读取 %d 条唯一 traceid 记录", total)

    # 确保目标表存在（如果不存在则创建）
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {args.target_table} (
            traceid STRING,
            keyword STRING,
            cityid STRING,
            userlat STRING,
            userlon STRING,
            pgpath STRING,
            caller STRING,
            dify_score INT,
            core_position INT,
            chaos INT,
            special_penalty INT
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS ORC
    """)

    # 收集所有结果（适合数据量不大，如几十万条；如果更大，需改用 foreachPartition 分批写入）
    # 由于 API 调用较慢，collect 到 Driver 是可行的，但要注意 Driver 内存。
    # 这里我们直接 collect 后逐条处理，然后一次性写入。
    rows = df.collect()
    all_results = []

    for row in rows:
        inputs = {
            "keyword": row.keyword,
            "cityid": str(row.cityid),
            "userlat": str(row.userlat),
            "userlon": str(row.userlon),
            "pgpath": row.pgpath,
            "caller": row.caller,
        }
        outputs = call_dify_workflow(
            args.dify_api_key,
            args.dify_workflow_id,
            args.dify_url,
            inputs,
        )
        if outputs:
            score, core_pos, chaos, special_penalty = parse_dify_output(outputs)
        else:
            score = core_pos = chaos = special_penalty = None

        all_results.append({
            "traceid": row.traceid,
            "keyword": row.keyword,
            "cityid": row.cityid,
            "userlat": row.userlat,
            "userlon": row.userlon,
            "pgpath": row.pgpath,
            "caller": row.caller,
            "dify_score": score,
            "core_position": core_pos,
            "chaos": chaos,
            "special_penalty": special_penalty,
            "year": year,
            "month": month,
            "day": day,
        })
        # 控制请求频率，避免触发限流
        time.sleep(0.1)

    # 将结果转换为 DataFrame 并写入 Hive
    if all_results:
        pdf = pd.DataFrame(all_results)
        result_df = spark.createDataFrame(pdf)
        result_df.write.mode("append").insertInto(args.target_table)
        logger.info("成功写入 %d 条记录到表 %s，分区 (%d, %d, %d)",
                    len(all_results), args.target_table, year, month, day)
    else:
        logger.warning("没有生成任何结果记录。")

    spark.stop()
    logger.info("任务完成。")


if __name__ == "__main__":
    main()
