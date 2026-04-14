# -*- encoding: utf-8 -*-
"""
脚本名称: dify_workflow_invoke.py
功能: 从 Hive 源表读取搜索日志数据，调用 Dify 工作流 API 进行模型排序评测，
     并将结果写入 Hive 目标表。
依赖: pyspark, requests, argparse
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, struct
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
    api_key: Optional[str],
    workflow_id: str,
    dify_url: str,
    inputs: Dict[str, str],
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Optional[Dict]:
    """
    调用 Dify 工作流 API（同步执行模式），支持公开访问（无认证）。

    Args:
        api_key: Dify API 密钥；若为 None 或空字符串，则不添加 Authorization 头
        workflow_id: 工作流 ID
        dify_url: Dify 服务基础地址（如 https://dify.17usoft.com）
        inputs: 工作流输入参数（dict）
        max_retries: 最大重试次数
        retry_delay: 初始重试间隔（秒）

    Returns:
        成功时返回 API 响应中的 outputs 部分（dict）；失败返回 None
    """
    url = f"{dify_url.rstrip('/')}/workflows/{workflow_id}/run"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "inputs": inputs,
        "response_mode": "blocking",
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                # Dify 同步返回结构通常为 {"data": {"outputs": {...}}}
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


# ---------- 批量处理函数（用于 foreachPartition） ----------
def process_partition(
    partition_rows,
    api_key: Optional[str],
    workflow_id: str,
    dify_url: str,
    result_table: str,
    target_year: int,
    target_month: int,
    target_day: int,
):
    """分区处理函数，同上，增加 dify_url 参数"""
    import pandas as pd
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.error("无法获取 SparkSession")
        return

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
            final_result = outputs.get("result", {})
            if isinstance(final_result, str):
                try:
                    final_result = json.loads(final_result)
                except:
                    pass
            score = final_result.get("评分", None)
            core_pos = final_result.get("核心结果位置", None)
            chaos = final_result.get("混乱度", None)
            special_penalty = final_result.get("特殊场景扣分", None)
            reason = final_result.get("评分依据", "")
        else:
            score = None
            core_pos = None
            chaos = None
            special_penalty = None
            reason = "API调用失败"

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
            "rating_reason": reason,
            "year": target_year,
            "month": target_month,
            "day": target_day,
        })

    if results:
        pdf = pd.DataFrame(results)
        df = spark.createDataFrame(pdf)
        df.write.mode("append").insertInto(result_table)


# ---------- 主函数 ----------
def main():
    parser = argparse.ArgumentParser(description="从 Hive 读取搜索日志，调用 Dify 工作流，写入结果表")
    parser.add_argument("--source-table", type=str, required=True, help="源 Hive 表名")
    parser.add_argument("--target-table", type=str, required=True, help="目标 Hive 表名")
    parser.add_argument("--dt", type=str, required=True, help="源表分区日期，格式 yyyyMMdd")
    parser.add_argument("--dify-workflow-id", type=str, required=True, help="Dify 工作流 ID")
    parser.add_argument("--dify-url", type=str, default="https://dify.17usoft.com", help="Dify API 基础地址")
    parser.add_argument("--dify-api-key", type=str, required=False, default=None, help="Dify API Key（公开访问可不提供）")
    parser.add_argument("--batch-size", type=int, default=100, help="每个 partition 内批量大小（控制内存）")
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

    # 读取源表（假设源表按 dt 分区，且包含以下字段）
    # 如果源表一个 traceid 对应多行，则按 traceid 分组取第一行（根据业务逻辑调整）
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

    # 如果存在重复 traceid，按任意规则去重（例如取第一条）
    df = df.dropDuplicates(["traceid"])

    logger.info("共读取 %d 条唯一 traceid 记录", df.count())

    # 定义目标表 schema（需提前创建）
    target_schema = StructType([
        StructField("traceid", StringType(), True),
        StructField("keyword", StringType(), True),
        StructField("cityid", StringType(), True),
        StructField("userlat", StringType(), True),
        StructField("userlon", StringType(), True),
        StructField("pgpath", StringType(), True),
        StructField("caller", StringType(), True),
        StructField("dify_score", IntegerType(), True),
        StructField("core_position", IntegerType(), True),
        StructField("chaos", IntegerType(), True),
        StructField("special_penalty", IntegerType(), True),
        StructField("rating_reason", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
    ])

    # 先创建空表（若不存在），实际可通过 Hive SQL 提前建表
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
            special_penalty INT,
            rating_reason STRING
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS ORC
    """)

    # 使用 foreachPartition 分布式调用 API
    # 注意：需要将 API 凭证广播到每个 executor（通过闭包变量）
    api_key = args.dify_api_key
    workflow_id = args.dify_workflow_id

    # 由于 broadcast 变量无法被 pickle 序列化（requests 模块不支持），
    # 直接通过闭包传递简单字符串是安全的（每个 executor 都会拿到副本）
    df.foreachPartition(
        lambda part: process_partition(
            part,
            args.dify_api_key,   # 可为 None
            args.dify_workflow_id,
            args.dify_url,
            args.target_table,
            year,
            month,
            day,
        )
    )

    logger.info("处理完成，结果已追加到 %s (分区 year=%d, month=%d, day=%d)",
                args.target_table, year, month, day)
    spark.stop()


if __name__ == "__main__":
    main()
