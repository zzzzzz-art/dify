# Dify Workflow Invoke

从 Hive 源表读取搜索日志数据，调用 Dify 工作流 API 进行模型排序评测，并将结果写入 Hive 目标表。

## 功能特性

- 从 Hive 表读取搜索日志数据
- 批量调用 Dify 工作流 API 进行模型排序评测
- 支持分区日期参数
- 支持 API 密钥认证和公开访问
- 结果自动写入 Hive 目标表（按日期分区）

## 依赖

- Python 3.8+
- PySpark
- requests

## 安装

```bash
pip install -r requirements.txt
```

## 使用方法

```bash
python difyoutput.py \
    --source-table <源Hive表名> \
    --target-table <目标Hive表名> \
    --dt <分区日期，格式 yyyyMMdd> \
    --dify-workflow-id <Dify工作流ID> \
    --dify-url <Dify API基础地址> \
    --dify-api-key <Dify API密钥> \
    --batch-size <批处理大小>
```

### 参数说明

| 参数 | 必填 | 说明 |
|------|------|------|
| `--source-table` | 是 | 源 Hive 表名 |
| `--target-table` | 是 | 目标 Hive 表名 |
| `--dt` | 是 | 源表分区日期，格式 `yyyyMMdd` |
| `--dify-workflow-id` | 是 | Dify 工作流 ID |
| `--dify-url` | 否 | Dify API 基础地址（默认: https://dify.17usoft.com） |
| `--dify-api-key` | 否 | Dify API Key（公开访问可不提供） |
| `--batch-size` | 否 | 每个 partition 内批量大小（默认: 100） |

### 示例

```bash
python difyoutput.py \
    --source-table search_log_source \
    --target-table search_log_result \
    --dt 20240414 \
    --dify-workflow-id workflow_123456 \
    --dify-url https://dify.17usoft.com \
    --dify-api-key your_api_key_here \
    --batch-size 100
```

## 项目结构

```
renwu/
├── difyoutput.py      # 主脚本文件
├── README.md          # 项目说明
├── requirements.txt   # 依赖列表
└── LICENSE            # 许可证
```

## 许可证

MIT License
