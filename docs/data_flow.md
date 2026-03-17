# EC2 Analysis Agent — End-to-End Data Flow


## High-Level Architecture

```mermaid
graph LR
    A["☁️ AWS CloudWatch"] --> B["📦 S3 Data Lake"]
    B --> C["🗄️ PostgreSQL"]
    C --> D["⚙️ FastAPI Backend"]
    D --> E["🤖 Groq LLM"]
    D --> F["💰 AWS Pricing MCP"]
    D --> G["🖥️ React Frontend"]
```

---

## Phase 1: Data Extraction — CloudWatch → S3

> **File:** `agent_backend/data/ec2_cloudwatch_metrics.py`

```mermaid
graph TD
    subgraph Phase1 ["Phase 1 — CloudWatch → S3 Parquet"]
        direction TB
        ENV["🔧 .env Config"] -->|AWS Credentials, Region, Bucket| INIT["Initialize AWS Clients"]
        INIT --> EC2_CLIENT["EC2 Client"]
        INIT --> CW_CLIENT["CloudWatch Client"]
        INIT --> S3_CLIENT["S3 Client"]

        EC2_CLIENT -->|describe_instances| DISCOVER["Discover EC2 Instances"]
        DISCOVER -->|"instance_id, type, name, AZ, state"| INST_LIST["Instance List"]

        INST_LIST --> PARALLEL["ThreadPoolExecutor (max 10 workers)"]

        PARALLEL --> FETCH_STD["Fetch Standard Metrics (AWS/EC2)"]
        PARALLEL --> FETCH_MEM["Fetch Memory Metrics (CWAgent)"]

        FETCH_STD -->|"CPUUtilization (Avg, Max, Min)\nCPUCreditUsage (Sum, Max, Min)\nCPUCreditBalance (Avg)\nCPUSurplusCreditBalance (Avg)\nCPUSurplusCreditsCharged (Sum)"| RAW["Raw Datapoints"]

        FETCH_MEM -->|"mem_used_percent (Avg, Max)\nmem_used (Avg, Max)\nmem_available (Avg, Min)\nmem_total (Avg)\nmem_cached (Avg)\nmem_buffered (Avg)"| RAW

        RAW --> RESTRUCTURE["Restructure DataFrame"]
        RESTRUCTURE -->|"20 columns:\nextracted_at, timestamp, instance_id,\ninstance_name, type, state, az,\nIPs, platform, launch_time,\nnamespace, category, metric_name,\nunit, period_sec,\nstat_average / maximum / minimum / sum"| PARQUET["Convert → Snappy Parquet"]

        PARQUET --> S3_UPLOAD["Upload to S3"]
        S3_UPLOAD -->|"s3://bucket/prefix/year=YYYY/month=MM/day=DD/metrics_HHMMSS.parquet"| S3_DONE["✅ S3 Data Lake"]
    end
```

### Key Details:
| Aspect | Detail |
|---|---|
| **API Call** | `cw_client.get_metric_statistics()` |
| **Lookback** | Configurable (default 5 days) |
| **Period** | 60 seconds (1 min granularity) |
| **Parallelism** | Up to 10 threads |
| **Partitioning** | Hive-style `year=/month=/day=/` |
| **Compression** | Snappy (optimized for Athena/Glue) |

---

## Phase 2: ETL — S3 Parquet → PostgreSQL

> **File:** `agent_backend/data/cloud_agent.py`

```mermaid
graph TD
    subgraph Phase2 ["Phase 2 — S3 → PostgreSQL ETL"]
        direction TB
        SCHEMA["Apply DDL Schema (ec2_metrics_schema.sql)"] --> CW_EXTRACT["Run Phase 1 (CloudWatch → S3)"]
        CW_EXTRACT --> WATERMARK_CHECK["Check etl_watermark Table"]
        WATERMARK_CHECK -->|"last_processed_file"| FILTER["Filter: Only New Files"]

        FILTER --> S3_READ["Read Parquet via s3fs"]
        S3_READ -->|"pd.read_parquet()"| RAW_DF["Raw DataFrame"]

        RAW_DF --> TRANSFORM["Transform: Daily Aggregation"]
        TRANSFORM -->|"GROUP BY instance_id, metric_name, day_bucket"| AGG_DF["Aggregated DataFrame"]

        AGG_DF --> UPSERT["Upsert to PostgreSQL"]
        UPSERT -->|"COPY → staging_metrics\nINSERT … ON CONFLICT UPDATE"| PG_METRICS["ec2_metrics_latest Table"]

        UPSERT --> WATERMARK_UPD["Update etl_watermark"]

        PG_METRICS --> VIEWS["PostgreSQL Views"]
        VIEWS --> V10["v_ec2_metrics_10d"]
        VIEWS --> V30["v_ec2_metrics_30d"]
        VIEWS --> V60["v_ec2_metrics_60d"]
        VIEWS --> V90["v_ec2_metrics_90d"]
        VIEWS --> VLLM["v_ec2_llm_summary"]

        PG_METRICS --> PRICE_SYNC["Pricing Sync"]
        PRICE_SYNC -->|"All distinct instance_types"| MCP_SYNC["cost_agent.sync_prices()"]
        MCP_SYNC --> PG_PRICES["ec2_instance_prices Table"]
    end
```

### Transformation Rules:
| Input Stat | Aggregation | Purpose |
|---|---|---|
| `stat_average` | `mean()` | Representative daily average |
| `stat_maximum` | `max()` | Absolute peak for the day |
| `stat_minimum` | `min()` | Absolute trough for the day |
| `stat_sum` | `sum()` | Cumulative (network/disk throughput) |
| `daily_active_hours` | `(max_ts - min_ts) / 3600` | Instance uptime per day |

---

## Phase 3: Pricing Resolution

> **File:** `agent_backend/agents/cost/cost_agent.py`

```mermaid
graph TD
    subgraph Phase3 ["Phase 3 — Pricing Resolution (3-Tier)"]
        direction TB
        REQ["Price Request for instance_type + region"]

        REQ --> TIER1{"Tier 1: Memory Cache"}
        TIER1 -->|HIT| DONE["✅ Return hourly_usd"]
        TIER1 -->|MISS| TIER2{"Tier 2: PostgreSQL (ec2_instance_prices)"}

        TIER2 -->|HIT| UPDATE_MEM1["Update Memory Cache"]
        UPDATE_MEM1 --> DONE

        TIER2 -->|MISS| TIER3["Tier 3: AWS Pricing MCP Server"]

        TIER3 --> MCP_SPAWN["Spawn MCP subprocess (stdio)"]
        MCP_SPAWN --> MCP_INIT["session.initialize()"]
        MCP_INIT --> MCP_TOOLS["session.list_tools()"]
        MCP_TOOLS --> TOOL_SELECT{"Select Pricing Tool"}

        TOOL_SELECT -->|"get_ec2_instance_price"| CALL_TOOL["session.call_tool()"]
        TOOL_SELECT -->|"get_pricing"| CALL_TOOL
        TOOL_SELECT -->|"search_products"| CALL_TOOL

        CALL_TOOL -->|"Filters: Linux, Shared, OnDemand"| AWS_API["AWS Pricing API"]
        AWS_API --> PARSE["_extract_price_from_tool_content()"]
        PARSE -->|"Recursive JSON search:\nhourly_usd, pricePerUnit.USD"| PRICE_VAL["Extracted Price"]

        PRICE_VAL --> SAVE_DB["Save to ec2_instance_prices"]
        PRICE_VAL --> UPDATE_MEM2["Update Memory Cache"]
        SAVE_DB --> DONE
        UPDATE_MEM2 --> DONE
    end
```

### Concurrency & Caching:
| Aspect | Detail |
|---|---|
| **Semaphore** | Global limit of 1 concurrent MCP subprocess |
| **Timeout** | 10 seconds per instance type lookup |
| **Cache TTL** | 24 hours in memory, permanent in PostgreSQL |
| **Batch Prefetch** | Always fetches ~50 common "rightsizing target" types |
| **Monthly Calc** | `hourly_usd × 730` (standard AWS month) |

---

## Phase 4: Analysis — User Request → LLM Streaming

> **Files:** `agent_backend/main.py`, `agent_backend/agents/analysis/analysis_agent.py`

```mermaid
graph TD
    subgraph Phase4 ["Phase 4 — Analysis Request Flow"]
        direction TB
        UI["🖥️ React Frontend"] -->|"POST /analyse\n{window_days, instance_ids, focus, question}"| API["FastAPI: /analyse"]

        API --> CACHE_CHECK{"Check analysis_cache"}
        CACHE_CHECK -->|HIT| STREAM_CACHED["Stream Cached Response (SSE)"]
        STREAM_CACHED --> UI

        CACHE_CHECK -->|MISS| FETCH_DB["database.fetch_metrics()"]
        FETCH_DB -->|"v_ec2_llm_summary view\nDISTINCT ON instance_id"| METRICS["Metric Rows"]

        METRICS --> PRICING["get_pricing_table_async()"]
        PRICING -->|"Phase 3 lookup"| PRICING_MD["Pricing Markdown Table"]

        METRICS --> FORMAT["PromptBuilder.format_metrics()"]
        PRICING_MD --> FORMAT
        FORMAT -->|"Markdown table:\nInstance ID, Name, Type, AZ,\nCPU Avg%, CPU P95%, Mem P95%,\nUptime (h), Samples"| DATA_MD["Formatted Data"]

        DATA_MD --> SYS_PROMPT["build_system_prompt(focus)"]
        DATA_MD --> USR_PROMPT["build_user_prompt()"]

        SYS_PROMPT -->|"Rightsizing rules\nRisk thresholds\nReport format"| LLM_CALL["LLMService.stream_response()"]
        USR_PROMPT -->|"Fleet data + pricing table + user question"| LLM_CALL

        LLM_CALL -->|"Groq API\nllama-3.3-70b-versatile\ntemp=0.001, max_tokens=4096"| GROQ["🤖 Groq Cloud"]
        GROQ -->|"Streaming chunks"| SSE["Server-Sent Events (SSE)"]
        SSE --> UI
        SSE -->|"Collect all tokens"| CACHE_SAVE["Save to analysis_cache"]
    end
```

---

## Phase 5: Cost Comparison & Savings Tracking

> **Files:** `agent_backend/main.py`, `agent_backend/agents/orchestrator/savings.py`

```mermaid
graph TD
    subgraph Phase5 ["Phase 5 — Savings Workflow"]
        direction TB
        LLM_OUT["LLM Markdown Output"] -->|"Recommendation Table"| PARSE["SavingsTracker.parse_recommendations()"]
        PARSE -->|"Regex: Instance ID, Current Type,\nRecommended Type, Saving $"| REC_MAP["Recommendation Map"]

        UI_SAVE["User clicks 'Save Recs'"] --> COMPARE["POST /pricing/compare-by-instance"]

        COMPARE --> RESOLVE_TYPE["_get_current_instance_type()"]
        RESOLVE_TYPE -->|"v_ec2_llm_summary → ec2_metrics_latest"| CUR_TYPE["Current Instance Type"]

        COMPARE --> UPTIME_LOOKUP["database.fetch_uptime()"]
        UPTIME_LOOKUP --> UPTIME_HRS["Uptime Hours"]

        CUR_TYPE --> COST_COMPARE["compare_instance_costs()"]
        UPTIME_HRS --> COST_COMPARE
        COST_COMPARE -->|"hourly_diff, monthly_diff,\nsavings_percent, usage_saving"| RESULT["Cost Comparison Result"]

        REC_MAP --> BULK_SAVE["POST /savings/bulk"]
        BULK_SAVE --> UPSERT_SAVINGS["UPSERT → savings_tracker"]
        UPSERT_SAVINGS -->|"instance_id, current_type,\nrecommended_type,\ncurrent/recommended monthly price,\nestimated saving, status"| PG_SAVINGS["savings_tracker Table"]

        PG_SAVINGS --> STATUS_UPDATE["PATCH /savings/:id/status"]
        STATUS_UPDATE -->|"Proposed → Investigating →\nImplemented / Rejected"| TRACK["📊 Savings Dashboard"]
    end
```

---

## Complete Database Schema

```mermaid
erDiagram
    ec2_metrics_latest {
        varchar instance_id PK
        varchar metric_name PK
        date day_bucket PK
        varchar instance_name
        varchar instance_type
        varchar az
        varchar platform
        float stat_average
        float stat_maximum
        float stat_minimum
        float stat_sum
        float daily_active_hours
        timestamp loaded_at
    }

    ec2_instance_prices {
        varchar instance_type PK
        varchar region PK
        float hourly_usd
        timestamp updated_at
    }

    analysis_cache {
        varchar cache_key PK
        text response_text
        timestamp created_at
    }

    savings_tracker {
        serial id PK
        varchar instance_id UK
        varchar instance_name
        varchar current_type
        varchar recommended_type
        text recommendation
        numeric current_monthly_cost_usd
        numeric recommended_monthly_cost_usd
        numeric estimated_monthly_saving_usd
        varchar status
        int window_days
        timestamp created_at
        timestamp updated_at
    }

    etl_watermark {
        varchar process_name PK
        varchar last_processed_file
    }

    ec2_metrics_latest ||--o{ savings_tracker : "instance_id"
    ec2_metrics_latest }o--|| ec2_instance_prices : "instance_type"
```

---

## API Route Map

| Route | Method | Source File | Purpose |
|---|---|---|---|
| `/health` | GET | `main.py` | Health check |
| `/instances` | GET | `main.py` → `database.py` | List all EC2 instances |
| `/fleet-summary` | GET | `main.py` → `database.py` | Fleet utilization overview |
| `/analyse` | POST | `main.py` → `analysis_agent.py` | Stream LLM analysis (SSE) |
| `/auto-select` | POST | `main.py` → LLM → `database.py` | NL → SQL instance selection |
| `/timeseries` | GET | `main.py` → `database.py` | Daily metric charts |
| `/timeseries-compare` | GET | `main.py` → `database.py` | Multi-instance comparison |
| `/pricing` | GET | `main.py` → `cost_agent.py` | On-demand pricing lookup |
| `/pricing/compare` | POST | `main.py` → `cost_agent.py` | Compare two instance costs |
| `/pricing/compare-by-instance` | POST | `main.py` → `cost_agent.py` | Compare with uptime costs |
| `/parse-recommendations` | POST | `main.py` → `savings.py` | Parse LLM markdown server-side |
| `/uptime` | GET | `main.py` → `database.py` | Fleet uptime + costs |
| `/uptime/{id}` | GET | `main.py` → `database.py` | Single instance uptime |
| `/savings` | POST/GET | `main.py` → `savings.py` | Create/list savings entries |
| `/savings/{id}/status` | PATCH | `main.py` → `savings.py` | Update recommendation status |
| `/savings/bulk` | POST | `main.py` → `savings.py` | Bulk save from LLM output |
