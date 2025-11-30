# Dota 2 Data Pipeline - Hybrid Engineer Project

## 📋 Tổng Quan Dự Án

Dự án này là một **Data Pipeline hoàn chỉnh** để thu thập, xử lý và phân tích dữ liệu từ game Dota 2 thông qua OpenDota API. Pipeline được xây dựng theo kiến trúc **Medallion Architecture** (Bronze-Silver-Gold) với orchestration bằng Apache Airflow và transformation bằng dbt.

---

## 🎯 Mục Tiêu Dự Án

1. **Thu thập dữ liệu tự động** từ OpenDota API về các trận đấu Dota 2
2. **Làm sạch và chuẩn hóa** dữ liệu thô
3. **Tạo các bảng phân tích** để hỗ trợ insights về:
   - Thống kê trận đấu (win rate, game mode, duration...)
   - Thống kê người chơi (KDA, GPM, XPM, hero performance...)
4. **Tự động hóa toàn bộ quy trình** từ ingestion đến transformation
5. **Export dữ liệu** sang CSV để dễ dàng phân tích hoặc chia sẻ

---

## 🏗️ Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────┐
│                      AIRFLOW ORCHESTRATION                       │
│                   (Docker Container: Scheduler)                  │
└─────────────────────────────────────────────────────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
┌───────────────┐      ┌───────────────┐      ┌──────────────────┐
│ DAG 1:        │      │ DAG 2:        │      │ DAG 3:           │
│ Refresh       │──┬──▶│ Ingest Match  │──┬──▶│ Transform &      │
│ Metadata      │  │   │ Details       │  │   │ Export           │
└───────────────┘  │   └───────────────┘  │   └──────────────────┘
                   │                      │
                   │   ┌──────────────────▼──────────────────┐
                   │   │     PostgreSQL Database             │
                   │   │  ┌──────────────────────────────┐  │
                   └──▶│  │  DOTA Schema (Dimensions)    │  │
                       │  │  - dim_heroes               │  │
                       │  │  - dim_game_modes           │  │
                       │  │  - dim_lobby_types          │  │
                       │  └──────────────────────────────┘  │
                       │                                     │
                       │  ┌──────────────────────────────┐  │
                       │  │  BRONZE Layer (Raw Data)     │  │
                       │  │  - matches (JSONB)           │  │
                       │  └──────────────────────────────┘  │
                       │           │                         │
                       │           │ dbt transformation      │
                       │           ▼                         │
                       │  ┌──────────────────────────────┐  │
                       │  │  SILVER Layer (Cleaned)      │  │
                       │  │  - stg_dota2_matches_raw     │  │
                       │  │  - silver_matches            │  │
                       │  │  - silver_players            │  │
                       │  └──────────────────────────────┘  │
                       │           │                         │
                       │           │ dbt aggregation         │
                       │           ▼                         │
                       │  ┌──────────────────────────────┐  │
                       │  │  GOLD Layer (Analytics)      │  │
                       │  │  - gold_match_analytics      │  │
                       │  │  - gold_player_stats         │  │
                       │  └──────────────────────────────┘  │
                       └─────────────────────────────────────┘
                                     │
                                     ▼
                       ┌─────────────────────────────┐
                       │  CSV Export (OneDrive Sync) │
                       │  - gold_match_analytics.csv │
                       │  - gold_player_stats.csv    │
                       └─────────────────────────────┘
```

---

## 🛠️ Tech Stack

### **Infrastructure & Orchestration**
- **Docker & Docker Compose**: Containerization và quản lý services
- **Apache Airflow 2.x**: Workflow orchestration
  - **LocalExecutor**: Chạy tasks đồng thời trên 1 máy
  - **PostgreSQL Backend**: Lưu metadata của Airflow

### **Database**
- **PostgreSQL 13**: Relational database
  - **JSONB Type**: Lưu trữ raw JSON data hiệu quả
  - **Schemas**: Phân tách logic theo layers (bronze, silver, gold, dota)

### **Data Transformation**
- **dbt (data build tool) 1.10.15**: SQL-based transformation framework
  - **Jinja Templating**: Dynamic SQL generation
  - **Materialization**: Tables, Views
  - **Macros**: Reusable SQL logic

### **Programming Languages**
- **Python 3.12**: Airflow DAGs, data ingestion scripts
  - `psycopg2`: PostgreSQL driver
  - `requests`: HTTP client cho API calls
- **SQL**: Data transformation logic

### **External APIs**
- **OpenDota API**: Free Dota 2 match data
  - Rate limit: 60 requests/minute (free tier)
  - Endpoints: `/publicMatches`, `/matches/{match_id}`

---

## 📊 Chi Tiết Database Schema

### **Tổng Số Bảng: 10 bảng chính**

#### **1. DOTA Schema (Dimension Tables) - 3 bảng**

##### `dota.dim_heroes`
```sql
CREATE TABLE dota.dim_heroes (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    localized_name VARCHAR(255),
    primary_attr VARCHAR(50),
    attack_type VARCHAR(50),
    roles TEXT[]
);
```
**Mục đích**: Lưu thông tin về 123 heroes trong Dota 2  
**Dữ liệu mẫu**: Anti-Mage (id=1), Crystal Maiden (id=5)  
**Nguồn**: OpenDota API `/heroes`

##### `dota.dim_game_modes`
```sql
CREATE TABLE dota.dim_game_modes (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    balanced BOOLEAN
);
```
**Mục đích**: Các chế độ chơi (All Pick, Ranked, Turbo...)  
**Dữ liệu mẫu**: All Pick (id=22), Ranked All Pick (id=22)

##### `dota.dim_lobby_types`
```sql
CREATE TABLE dota.dim_lobby_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255)
);
```
**Mục đích**: Loại phòng chơi (Normal, Ranked, Tournament...)  
**Dữ liệu mẫu**: Normal (id=0), Practice (id=1), Tournament (id=2)

---

#### **2. BRONZE Layer (Raw Data) - 1 bảng**

##### `bronze.matches`
```sql
CREATE TABLE bronze.matches (
    match_id BIGINT PRIMARY KEY,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bronze_matches_ingested ON bronze.matches(ingested_at);
```

**Mục đích**: Lưu trữ **RAW JSON data** từ OpenDota API  
**Kích thước**: ~125 matches hiện tại  
**Cấu trúc JSON**:
```json
{
  "match_id": 8094726008,
  "duration": 1847,
  "game_mode": 22,
  "lobby_type": 7,
  "radiant_win": true,
  "start_time": 1732937482,
  "players": [
    {
      "account_id": 1770607077,
      "hero_id": 1,
      "player_slot": 0,
      "kills": 10,
      "deaths": 5,
      "assists": 15,
      "gold_per_min": 450,
      "xp_per_min": 550,
      ...
    },
    ...
  ]
}
```

**Ý nghĩa các fields quan trọng**:
- `match_id`: ID duy nhất của trận đấu
- `duration`: Thời lượng trận (giây)
- `game_mode`: ID của game mode (join với `dim_game_modes`)
- `radiant_win`: `true` = Radiant thắng, `false` = Dire thắng
- `players[]`: Mảng 10 players (5 Radiant, 5 Dire)
- `player_slot`: 0-4 = Radiant, 128-132 = Dire

---

#### **3. SILVER Layer (Cleaned & Normalized) - 3 bảng**

##### `silver.stg_dota2_matches_raw` (VIEW)
```sql
CREATE VIEW silver.stg_dota2_matches_raw AS
SELECT 
  match_id,
  raw_data,
  ingested_at
FROM bronze.matches;
```
**Mục đích**: **Staging view** - Đơn giản pass-through từ bronze  
**Materialization**: VIEW (không chiếm storage)  
**Role**: Entry point cho các transformations tiếp theo

---

##### `silver.silver_matches` (TABLE)
```sql
CREATE TABLE silver.silver_matches AS
SELECT
    match_id,
    TO_TIMESTAMP((raw_data->>'start_time')::BIGINT) as match_datetime,
    (raw_data->>'duration')::INTEGER as duration_seconds,
    (raw_data->>'duration')::INTEGER / 60.0 as duration_minutes,
    (raw_data->>'radiant_win')::BOOLEAN as radiant_win,
    (raw_data->>'game_mode')::INTEGER as game_mode,
    (raw_data->>'lobby_type')::INTEGER as lobby_type,
    raw_data->'players' as players_json,
    ingested_at
FROM silver.stg_dota2_matches_raw;
```

**Mục đích**: **Parse JSON** và extract các fields quan trọng ở match level  
**Kích thước**: 125 rows (1 row = 1 match)  
**Transformations**:
- `TO_TIMESTAMP()`: Convert Unix timestamp → PostgreSQL timestamp
- `::INTEGER`: Cast string to integer
- `::BOOLEAN`: Cast string to boolean
- `->` vs `->>`: `->` giữ JSON type, `->>` convert sang text

**Use cases**:
- Base table cho match-level analytics
- Join với dimension tables qua `game_mode`, `lobby_type`

---

##### `silver.silver_players` (TABLE)
```sql
CREATE TABLE silver.silver_players AS
WITH unnested_players AS (
    SELECT
        match_id,
        match_datetime,
        duration_seconds,
        radiant_win,
        game_mode,
        lobby_type,
        jsonb_array_elements(players_json) as player_data
    FROM silver.silver_matches
)
SELECT
    match_id,
    match_datetime,
    duration_seconds,
    radiant_win,
    game_mode,
    lobby_type,
    (player_data->>'account_id')::BIGINT as account_id,
    (player_data->>'hero_id')::INTEGER as hero_id,
    (player_data->>'player_slot')::INTEGER as player_slot,
    (player_data->>'kills')::INTEGER as kills,
    (player_data->>'deaths')::INTEGER as deaths,
    (player_data->>'assists')::INTEGER as assists,
    (player_data->>'gold_per_min')::INTEGER as gold_per_min,
    (player_data->>'xp_per_min')::INTEGER as xp_per_min,
    (player_data->>'level')::INTEGER as level,
    (player_data->>'hero_damage')::INTEGER as hero_damage,
    (player_data->>'tower_damage')::INTEGER as tower_damage,
    (player_data->>'hero_healing')::INTEGER as hero_healing,
    (player_data->>'last_hits')::INTEGER as last_hits,
    (player_data->>'denies')::INTEGER as denies,
    -- Logic xác định player thắng hay thua
    CASE
        WHEN (player_data->>'player_slot')::INTEGER < 128 THEN radiant_win
        ELSE NOT radiant_win
    END as player_won
FROM unnested_players
WHERE (player_data->>'account_id') IS NOT NULL;
```

**Mục đích**: **Unnest players array** - Chuyển từ 1 row (match) → 10 rows (players)  
**Kích thước**: 912 rows (125 matches × ~7.3 players/match average)  
**Transformations**:
- `jsonb_array_elements()`: Phá array thành rows
- `CASE WHEN player_slot < 128`: Logic xác định team
  - Radiant: slots 0-4 (player_slot < 128)
  - Dire: slots 128-132 (player_slot >= 128)
- `player_won`: Kết hợp `radiant_win` + `player_slot` để xác định win/loss

**Use cases**:
- Player-level analytics
- Hero performance analysis
- Join với `dim_heroes` qua `hero_id`

---

#### **4. GOLD Layer (Analytics & Aggregations) - 2 bảng**

##### `gold.gold_match_analytics` (TABLE)
```sql
CREATE TABLE gold.gold_match_analytics AS
SELECT
    m.match_id,
    m.match_datetime,
    m.duration_minutes,
    m.radiant_win,
    gm.name as game_mode_name,
    gm.balanced as is_balanced_mode,
    lt.name as lobby_type_name,
    -- Player statistics aggregations
    COUNT(DISTINCT p.account_id) as total_players,
    ROUND(AVG(p.kills), 2) as avg_kills,
    ROUND(AVG(p.deaths), 2) as avg_deaths,
    ROUND(AVG(p.assists), 2) as avg_assists,
    ROUND(AVG(p.gold_per_min), 0) as avg_gpm,
    ROUND(AVG(p.xp_per_min), 0) as avg_xpm,
    ROUND(AVG(p.hero_damage), 0) as avg_hero_damage,
    m.ingested_at
FROM silver.silver_matches m
LEFT JOIN silver.silver_players p ON m.match_id = p.match_id
LEFT JOIN dota.dim_game_modes gm ON m.game_mode = gm.id
LEFT JOIN dota.dim_lobby_types lt ON m.lobby_type = lt.id
GROUP BY m.match_id, m.match_datetime, m.duration_minutes, 
         m.radiant_win, gm.name, gm.balanced, lt.name, m.ingested_at;
```

**Mục đích**: **Match-level analytics** với dimension enrichment  
**Kích thước**: 125 rows (1 row = 1 match)  
**Transformations**:
- **JOINs**: Kết hợp 4 bảng (silver_matches + silver_players + dim_game_modes + dim_lobby_types)
- **Aggregations**: `AVG()`, `COUNT(DISTINCT)`, `ROUND()`
- **Enrichment**: Thêm tên readable từ dimension tables

**Business value**:
- Phân tích xu hướng theo game mode
- So sánh balanced vs unbalanced modes
- Tracking average performance metrics theo thời gian

---

##### `gold.gold_player_stats` (TABLE)
```sql
CREATE TABLE gold.gold_player_stats AS
SELECT
    p.account_id,
    p.hero_id,
    h.localized_name as hero_name,
    COUNT(*) as total_matches,
    SUM(CASE WHEN p.player_won THEN 1 ELSE 0 END) as wins,
    ROUND(100.0 * SUM(CASE WHEN p.player_won THEN 1 ELSE 0 END) / COUNT(*), 2) as win_rate_pct,
    ROUND(AVG(p.kills), 2) as avg_kills,
    ROUND(AVG(p.deaths), 2) as avg_deaths,
    ROUND(AVG(p.assists), 2) as avg_assists,
    ROUND(AVG(p.kills + p.assists) / NULLIF(AVG(p.deaths), 0), 2) as kda_ratio,
    ROUND(AVG(p.gold_per_min), 0) as avg_gpm,
    ROUND(AVG(p.xp_per_min), 0) as avg_xpm,
    ROUND(AVG(p.hero_damage), 0) as avg_hero_damage,
    ROUND(AVG(p.last_hits), 0) as avg_last_hits
FROM silver.silver_players p
LEFT JOIN dota.dim_heroes h ON p.hero_id = h.id
WHERE p.account_id IS NOT NULL
GROUP BY p.account_id, p.hero_id, h.localized_name
HAVING COUNT(*) >= 1
ORDER BY total_matches DESC, win_rate_pct DESC;
```

**Mục đích**: **Player-hero performance statistics**  
**Kích thước**: 907 rows (unique combinations of account_id + hero_id)  
**Transformations**:
- **GROUP BY**: `account_id + hero_id` (mỗi player có thể chơi nhiều heroes)
- **Conditional aggregation**: `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
- **KDA calculation**: `(K + A) / D` với `NULLIF()` để tránh chia cho 0
- **HAVING**: Filter ra players có ít nhất 1 match

**Metrics**:
- `total_matches`: Số trận chơi với hero này
- `wins`: Số trận thắng
- `win_rate_pct`: % thắng
- `kda_ratio`: Kill-Death-Assist ratio (càng cao càng tốt)
- `avg_gpm` (Gold Per Minute): Thu nhập vàng/phút
- `avg_xpm` (Experience Per Minute): Kinh nghiệm/phút

**Business value**:
- Xác định "best heroes" của mỗi player
- Phân tích meta (heroes nào strong)
- Player profiling và recommendations

---

## 🔄 Data Flow Chi Tiết

### **Flow 1: Metadata Refresh (DAG: `refresh_metadata`)**

```
OpenDota API                          PostgreSQL
     │                                      │
     ├─ GET /heroes ──────────┐             │
     ├─ GET /constants/      │             │
     │  game_modes ──────────┼────────▶ TRUNCATE + INSERT
     └─ GET /constants/      │             │
        lobby_type ──────────┘             │
                                           │
                                   ┌───────▼────────┐
                                   │ dota.dim_heroes│
                                   │ dota.dim_game_ │
                                   │      modes     │
                                   │ dota.dim_lobby_│
                                   │      types     │
                                   └────────────────┘
```

**Kỹ thuật áp dụng**:
- **TRUNCATE CASCADE**: Xóa toàn bộ data cũ trước khi insert mới
- **Bulk INSERT**: Insert many rows trong 1 transaction
- **Error handling**: Retry logic với exponential backoff khi rate limit
- **Validation**: Kiểm tra response type trước khi process

**Code snippet quan trọng**:
```python
def fetch_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        response = requests.get(url, timeout=30)
        if response.status_code == 429:  # Rate limited
            wait_time = 60 * (attempt + 1)  # 60s, 120s, 180s
            time.sleep(wait_time)
            continue
        response.raise_for_status()
        return response
```

---

### **Flow 2: Match Ingestion (DAG: `ingest_match_details`)**

```
OpenDota API                          PostgreSQL
     │                                      │
     ├─ GET /publicMatches ──┐              │
     │  ?min_match_id=X      │              │
     └───────────────────────┼─────────▶ Check existing
                             │             match_ids
                             │                │
          ┌──────────────────┘                │
          │                                   │
          ├─ GET /matches/{id1}               │
          ├─ GET /matches/{id2}               │
          ├─ GET /matches/{id3}               │
          │  ... (10 matches per run)         │
          │                                   │
          └───────────────────────────────────▶ INSERT INTO
                  (sleep 3s between requests)   bronze.matches
                                                 (JSONB)
```

**Kỹ thuật áp dụng**:
- **Incremental loading**: Sử dụng Airflow Variable `last_match_id` để track progress
- **Deduplication**: Check existing IDs trước khi fetch
- **Rate limiting**: Sleep 3 giây giữa các requests (20 req/min < 60 req/min limit)
- **Batch processing**: 10 matches/run để tránh timeout
- **ON CONFLICT DO NOTHING**: Tránh duplicate inserts

**Code snippet quan trọng**:
```python
# Get existing match IDs to avoid duplicates
cursor.execute("SELECT match_id FROM bronze.matches")
existing_ids = set(row[0] for row in cursor.fetchall())

# Filter new matches
new_match_ids = [m['match_id'] for m in public_matches 
                 if m['match_id'] not in existing_ids]

# Insert with conflict handling
cursor.execute("""
    INSERT INTO bronze.matches (match_id, raw_data)
    VALUES (%s, %s::jsonb)
    ON CONFLICT (match_id) DO NOTHING
""", (match_id, json.dumps(match_details)))
```

---

### **Flow 3: Transformation & Export (DAG: `transform_and_export`)**

```
PostgreSQL                            dbt                PostgreSQL
    │                                  │                     │
    │                                  │                     │
bronze.matches ──────────────────▶ Parse JSON ─────────▶ silver.stg_dota2_
    │                              (staging)                matches_raw
    │                                  │                     (VIEW)
    │                                  │                     │
    │                                  ▼                     │
    │                            Extract fields              │
    │                            - match_id                  │
    │                            - duration      ─────────▶ silver.silver_
    │                            - radiant_win               matches
    │                            - game_mode                 (TABLE)
    │                            ...                         │
    │                                  │                     │
    │                                  ▼                     │
    │                          Unnest players[]              │
    │                          jsonb_array_     ──────────▶ silver.silver_
    │                          elements()                    players
    │                                  │                     (TABLE)
    │                                  │                     │
    │                                  ▼                     │
    │                           Aggregate by                 │
    │                           match_id        ──────────▶ gold.gold_match_
    │                           + JOIN dims                  analytics
    │                                  │                     (TABLE)
    │                                  │                     │
    │                                  ▼                     │
    │                           Aggregate by                 │
    │                           account_id      ──────────▶ gold.gold_player_
    │                           + hero_id                    stats
    │                                  │                     (TABLE)
    │                                  │                     │
    │                                  ▼                     │
    │                           Export to CSV                │
    └───────────────────────────────────────────────────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │ /opt/airflow/export/ │
                          │ - gold_match_        │
                          │   analytics.csv      │
                          │ - gold_player_       │
                          │   stats.csv          │
                          └──────────────────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │   OneDrive Sync      │
                          │ (C:/Users/.../Data)  │
                          └──────────────────────┘
```

**Kỹ thuật áp dụng trong dbt**:

1. **Materialization Strategy**:
   - **Views** (staging): Không chiếm storage, query trực tiếp từ bronze
   - **Tables** (silver, gold): Persist data để tăng performance

2. **Incremental Loading Check**:
```python
def check_new_data(**context):
    # Count bronze vs silver matches
    cursor.execute("SELECT COUNT(*) FROM bronze.matches")
    bronze_count = cursor.fetchone()[0]
    
    # Check if silver table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'silver' 
            AND table_name = 'silver_matches'
        )
    """)
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        # First run - always execute dbt
        return bronze_count
    
    # Compare counts
    cursor.execute("SELECT COUNT(*) FROM silver.silver_matches")
    silver_count = cursor.fetchone()[0]
    
    if bronze_count <= silver_count:
        raise ValueError("No new data")  # Skip dbt run
```

3. **Schema Override Macro**:
```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```
**Tại sao cần**: Mặc định dbt append custom schema vào target schema  
→ `silver_gold` thay vì `gold`  
→ Macro này override để dùng exact schema name

---

## 🎯 Workflow Execution Flow

### **Thứ tự chạy khi trigger `dota2_workflow_controller`**:

```
START
  │
  ▼
┌──────────────────────────────────┐
│ Task 1: trigger_refresh_metadata │
│ ├─ Triggers: refresh_metadata    │
│ └─ wait_for_completion: True     │
└──────────────────────────────────┘
  │ (chờ refresh_metadata hoàn thành)
  ▼
┌──────────────────────────────────┐
│ Task 2: trigger_ingest           │
│ ├─ Triggers: ingest_match_details│
│ └─ wait_for_completion: True     │
└──────────────────────────────────┘
  │ (chờ ingest_match_details hoàn thành)
  ▼
┌──────────────────────────────────┐
│ Task 3: trigger_transform        │
│ ├─ Triggers: transform_and_export│
│ └─ wait_for_completion: True     │
└──────────────────────────────────┘
  │
  ▼
END
```

**Code của Controller DAG**:
```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Task 1
trigger_refresh_metadata = TriggerDagRunOperator(
    task_id='trigger_refresh_metadata',
    trigger_dag_id='refresh_metadata',
    wait_for_completion=True,  # Đợi xong mới chạy tiếp
    poke_interval=30,           # Check status mỗi 30s
    dag=dag,
)

# Task 2
trigger_ingest = TriggerDagRunOperator(
    task_id='trigger_ingest',
    trigger_dag_id='ingest_match_details',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# Task 3
trigger_transform = TriggerDagRunOperator(
    task_id='trigger_transform',
    trigger_dag_id='transform_and_export',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# Dependencies
trigger_refresh_metadata >> trigger_ingest >> trigger_transform
```

---

## 🚀 Hướng Dẫn Sử Dụng

### **1. Khởi động hệ thống**

```bash
cd "c:/kiet.nguyen@ecentric/Hybrid Engineer"

# Start tất cả services
docker-compose up -d

# Kiểm tra services đã chạy chưa
docker ps
```

**Output mong đợi**:
```
CONTAINER ID   IMAGE                             STATUS
a12b9f4aaa5e   postgres:13-bullseye              Up 2 minutes (healthy)
266154e46538   hybridengineer-airflow-webserver  Up 2 minutes
0f248ecd3d3d   hybridengineer-airflow-scheduler  Up 2 minutes
3c8d9f2e1234   hybridengineer-dbt                Up 2 minutes
```

### **2. Truy cập Airflow UI**

1. Mở browser và truy cập: `http://localhost:8080`
2. Login:
   - **Username**: `admin`
   - **Password**: `admin`

### **3. Chạy pipeline lần đầu (Full Run)**

**Cách 1: Qua UI**
1. Vào tab **DAGs**
2. Tìm DAG: `dota2_workflow_controller`
3. Bấm nút ▶️ (Play) bên phải
4. Confirm trigger

**Cách 2: Qua CLI**
```bash
docker exec dota2_airflow_scheduler airflow dags trigger dota2_workflow_controller
```

**Thời gian chạy ước tính**:
- `refresh_metadata`: ~30 giây
- `ingest_match_details`: ~45 giây (10 matches × 3s delay)
- `transform_and_export`: ~60 giây (dbt run + export)
- **Tổng**: ~2-3 phút

### **4. Monitoring & Troubleshooting**

**Kiểm tra log của DAG**:
```bash
# Log của controller
docker exec dota2_airflow_scheduler airflow dags list-runs -d dota2_workflow_controller

# Log của task cụ thể
docker logs dota2_airflow_scheduler --tail 100 | grep "dota2_workflow_controller"
```

**Kiểm tra dữ liệu trong database**:
```bash
docker exec dota2_postgres psql -U airflow -d airflow -c "
SELECT 
    'bronze.matches' as table_name, COUNT(*) FROM bronze.matches
UNION ALL
SELECT 'silver.silver_matches', COUNT(*) FROM silver.silver_matches
UNION ALL
SELECT 'silver.silver_players', COUNT(*) FROM silver.silver_players
UNION ALL
SELECT 'gold.gold_match_analytics', COUNT(*) FROM gold.gold_match_analytics
UNION ALL
SELECT 'gold.gold_player_stats', COUNT(*) FROM gold.gold_player_stats;
"
```

**Output mong đợi**:
```
        table_name         | count
---------------------------+-------
 bronze.matches            |   125
 silver.silver_matches     |   125
 silver.silver_players     |   912
 gold.gold_match_analytics |   125
 gold.gold_player_stats    |   907
```

### **5. Chạy từng DAG riêng lẻ (Manual)**

```bash
# Chỉ refresh metadata
docker exec dota2_airflow_scheduler airflow dags trigger refresh_metadata

# Chỉ ingest matches
docker exec dota2_airflow_scheduler airflow dags trigger ingest_match_details

# Chỉ transform & export
docker exec dota2_airflow_scheduler airflow dags trigger transform_and_export
```

### **6. Xem kết quả CSV**

File CSV được export tự động vào 2 nơi:

1. **Trong container**:
   - `/opt/airflow/export/gold_match_analytics.csv`
   - `/opt/airflow/export/gold_player_stats.csv`

2. **OneDrive Sync** (nếu đã mount):
   - `C:/Users/Admin/OneDrive - exData/Data/gold_match_analytics.csv`
   - `C:/Users/Admin/OneDrive - exData/Data/gold_player_stats.csv`

**Copy từ container ra host**:
```bash
docker cp dota2_airflow_scheduler:/opt/airflow/export/gold_match_analytics.csv ./
docker cp dota2_airflow_scheduler:/opt/airflow/export/gold_player_stats.csv ./
```

### **7. Dừng hệ thống**

```bash
# Stop nhưng giữ data
docker-compose stop

# Stop và xóa containers (giữ data trong volumes)
docker-compose down

# Stop và xóa cả data (CẨNTHẬN!)
docker-compose down -v
```

---

## 🔍 Mối Quan Hệ Giữa Các Bảng

### **Relationship Diagram**

```
┌──────────────────┐
│  dota.dim_heroes │
│  (123 rows)      │
└────────┬─────────┘
         │ hero_id
         │
         ▼
┌──────────────────────────┐         ┌────────────────────────┐
│ silver.silver_players    │◄────────│ silver.silver_matches  │
│ (912 rows)               │1      n │ (125 rows)             │
│ - match_id               │         │ - match_id (PK)        │
│ - account_id             │         │ - duration             │
│ - hero_id (FK)───────────┤         │ - radiant_win          │
│ - kills, deaths, assists │         │ - game_mode (FK)───────┤
│ - gold_per_min           │         │ - lobby_type (FK)──────┤
│ - player_won             │         └────────────────────────┘
└────────┬─────────────────┘                   │              │
         │                                     │              │
         │                                     ▼              ▼
         │                          ┌──────────────────┐ ┌──────────────────┐
         │                          │ dota.dim_game_   │ │ dota.dim_lobby_  │
         │                          │ modes (13 rows)  │ │ types (9 rows)   │
         │                          └──────────────────┘ └──────────────────┘
         │
         │ GROUP BY match_id        │ GROUP BY account_id, hero_id
         ▼                          ▼
┌──────────────────────────┐ ┌──────────────────────────┐
│ gold.gold_match_         │ │ gold.gold_player_stats   │
│ analytics (125 rows)     │ │ (907 rows)               │
│ - match_id (PK)          │ │ - account_id, hero_id    │
│ - avg_kills, avg_deaths  │ │ - total_matches          │
│ - avg_gpm, avg_xpm       │ │ - wins, win_rate_pct     │
│ - game_mode_name         │ │ - avg_kills, avg_kda     │
│ - lobby_type_name        │ │ - hero_name              │
└──────────────────────────┘ └──────────────────────────┘
```

### **Foreign Key Relationships**

| Child Table              | Column       | Parent Table           | Parent Column |
|--------------------------|--------------|------------------------|---------------|
| `silver.silver_matches`  | `game_mode`  | `dota.dim_game_modes`  | `id`          |
| `silver.silver_matches`  | `lobby_type` | `dota.dim_lobby_types` | `id`          |
| `silver.silver_players`  | `match_id`   | `silver.silver_matches`| `match_id`    |
| `silver.silver_players`  | `hero_id`    | `dota.dim_heroes`      | `id`          |
| `gold.gold_match_analytics` | Inherited from silver layers | - | - |
| `gold.gold_player_stats` | Inherited from silver layers | - | - |

> **Lưu ý**: Foreign keys không được enforce ở database level (để tăng tốc độ insert), nhưng được đảm bảo bởi logic trong dbt transformations.

---

## 📈 Business Insights Có Thể Trích Xuất

### **1. Match Analytics**

```sql
-- Top 10 game modes phổ biến nhất
SELECT 
    game_mode_name,
    COUNT(*) as total_matches,
    ROUND(AVG(duration_minutes), 1) as avg_duration_min,
    ROUND(100.0 * SUM(CASE WHEN radiant_win THEN 1 ELSE 0 END) / COUNT(*), 1) as radiant_win_rate
FROM gold.gold_match_analytics
GROUP BY game_mode_name
ORDER BY total_matches DESC
LIMIT 10;
```

### **2. Player Performance**

```sql
-- Top 10 players giỏi nhất với hero cụ thể
SELECT 
    account_id,
    hero_name,
    total_matches,
    win_rate_pct,
    kda_ratio,
    avg_gpm
FROM gold.gold_player_stats
WHERE total_matches >= 2
ORDER BY win_rate_pct DESC, kda_ratio DESC
LIMIT 10;
```

### **3. Hero Meta Analysis**

```sql
-- Heroes nào được pick nhiều nhất và win rate ra sao?
SELECT 
    hero_name,
    SUM(total_matches) as times_picked,
    ROUND(AVG(win_rate_pct), 1) as avg_win_rate,
    ROUND(AVG(kda_ratio), 2) as avg_kda
FROM gold.gold_player_stats
GROUP BY hero_name
HAVING SUM(total_matches) >= 5
ORDER BY times_picked DESC;
```

---

## 🛡️ Best Practices & Design Patterns

### **1. Medallion Architecture**
- **Bronze**: Raw data, immutable
- **Silver**: Cleaned, normalized
- **Gold**: Business-optimized, aggregated

### **2. Idempotency**
- All DAGs can be re-run safely
- `ON CONFLICT DO NOTHING` prevents duplicates
- `TRUNCATE` ensures clean state for dimensions

### **3. Incremental Loading**
- Track `last_match_id` in Airflow Variable
- Only process new matches
- Skip dbt if no new data

### **4. Error Handling**
- Retry logic với exponential backoff
- Rate limit handling (429 status code)
- Transaction rollback on failures

### **5. Performance Optimization**
- Views for staging (no storage overhead)
- Tables for analytics (fast query)
- Indexes on frequently joined columns

---

## 🔧 Troubleshooting Common Issues

### **Issue 1: DAG không chạy**

**Triệu chứng**: DAG ở trạng thái "paused" hoặc không trigger được

**Giải pháp**:
```bash
# Unpause DAG
docker exec dota2_airflow_scheduler airflow dags unpause dota2_workflow_controller
docker exec dota2_airflow_scheduler airflow dags unpause refresh_metadata
docker exec dota2_airflow_scheduler airflow dags unpause ingest_match_details
docker exec dota2_airflow_scheduler airflow dags unpause transform_and_export
```

### **Issue 2: Rate limit từ OpenDota API**

**Triệu chứng**: Task `fetch_match_details` failed với HTTP 429

**Giải pháp**:
- Code đã có retry logic, đợi 2-6 phút tự động retry
- Hoặc giảm `batch_size` từ 10 xuống 5 trong `ingest_match_details.py`

### **Issue 3: dbt run failed**

**Triệu chứng**: Task `dbt_run` failed

**Debug**:
```bash
# Chạy dbt thủ công để xem lỗi chi tiết
docker exec dota2_dbt dbt run --project-dir /dbt/hybrid_engineer --profiles-dir /root/.dbt

# Kiểm tra dbt logs
docker exec dota2_dbt dbt debug --project-dir /dbt/hybrid_engineer --profiles-dir /root/.dbt
```

### **Issue 4: Không có dữ liệu trong gold tables**

**Triệu chứng**: `gold.gold_player_stats` có 0 rows

**Kiểm tra**:
```bash
# Check silver layer có data không
docker exec dota2_postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM silver.silver_players;"

# Check dim_heroes có data không (cần cho JOIN)
docker exec dota2_postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM dota.dim_heroes;"
```

**Giải pháp**: Chạy lại `refresh_metadata` DAG để populate dim tables

---

## 📞 Support & Contact

- **Project Owner**: Kiet Nguyen
- **Email**: gpt4work.data@gmail.com
- **Airflow UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (user: airflow, password: airflow)

---

## 📚 References

- [OpenDota API Documentation](https://docs.opendota.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [PostgreSQL JSONB](https://www.postgresql.org/docs/current/datatype-json.html)

---

**Version**: 1.0.0  
**Last Updated**: 2025-11-30  
**Status**: ✅ Production Ready
