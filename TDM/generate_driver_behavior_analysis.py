from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text
import pandas as pd
from datetime import datetime

# 数据库连接信息
username = 'yjy'
password = 'Yjy123456'
host = 'rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com'
port = '5432'
database = 'dataplatform'

# 连接数据库
engine = create_engine(
    f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',
    connect_args={'options': '-c client_encoding=UTF8'}
)


def create_driver_analysis_table():
    """创建司机行为分析结果表"""
    try:
        print("Using DB:", engine.url)
        with engine.begin() as conn:
            print('Creating driver analysis table...')
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.driver_behavior_analysis (
                    driver_id TEXT PRIMARY KEY,
                    total_trips INTEGER,
                    avg_trip_duration INTEGER,  -- 平均行程时间(秒)
                    max_trip_duration INTEGER,  -- 最长行程时间(秒)
                    min_trip_duration INTEGER,  -- 最短行程时间(秒)
                    total_driving_time INTEGER, -- 总驾驶时间(秒)
                    morning_trips INTEGER,      -- 早上行程(5:00-11:59)
                    afternoon_trips INTEGER,    -- 下午行程(12:00-17:59)
                    evening_trips INTEGER,      -- 晚上行程(18:00-21:59)
                    night_trips INTEGER,        -- 夜间行程(22:00-4:59)
                    --last_updated TIMESTAMP
                )
            """))
            print('Truncating table...')
            conn.execute(text("TRUNCATE TABLE public.driver_behavior_analysis"))
        print('Driver analysis table created successfully.')
    except Exception as e:
        print("Error creating table:", e)


def analyze_driver_behavior():
    """分析司机驾驶行为并将结果存入分析表"""
    try:
        # 从原始表获取数据
        query = """
        SELECT 
            devid AS driver_id,
            COUNT(*) AS total_trips,
            AVG(travel_time) AS avg_trip_duration,
            MAX(travel_time) AS max_trip_duration,
            MIN(travel_time) AS min_trip_duration,
            SUM(travel_time) AS total_driving_time,
            SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 5 AND 11 THEN 1 ELSE 0 END) AS morning_trips,
            SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS afternoon_trips,
            SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 18 AND 21 THEN 1 ELSE 0 END) AS evening_trips,
            SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 22 AND 23 
                 OR EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 0 AND 4 THEN 1 ELSE 0 END) AS night_trips
        FROM dwd_trip_info
        GROUP BY driver_id
        """

        with engine.connect() as conn:
            print("获取驾驶行为数据...")
            df = pd.read_sql(query, conn)

            # 插入数据到分析表
            print(f"分析了 {len(df)} 位司机的驾驶行为")

            # 逐行插入分析结果
            insert_count = 0
            with engine.begin() as transaction:
                for _, row in df.iterrows():
                    transaction.execute(text("""
                        INSERT INTO public.driver_behavior_analysis
                        (driver_id, total_trips, avg_trip_duration, max_trip_duration, 
                         min_trip_duration, total_driving_time, morning_trips, 
                         afternoon_trips, evening_trips, night_trips, last_updated)
                        VALUES (:driver_id, :total_trips, :avg_trip_duration, :max_trip_duration, 
                                :min_trip_duration, :total_driving_time, :morning_trips, 
                                :afternoon_trips, :evening_trips, :night_trips, :last_updated)
                    """), {
                        'driver_id': row['driver_id'],
                        'total_trips': int(row['total_trips']),
                        'avg_trip_duration': int(row['avg_trip_duration']),
                        'max_trip_duration': int(row['max_trip_duration']),
                        'min_trip_duration': int(row['min_trip_duration']),
                        'total_driving_time': int(row['total_driving_time']),
                        'morning_trips': int(row['morning_trips']),
                        'afternoon_trips': int(row['afternoon_trips']),
                        'evening_trips': int(row['evening_trips']),
                        'night_trips': int(row['night_trips']),
                        'last_updated': datetime.now()
                    })
                    insert_count += 1

            print(f"成功写入 {insert_count} 条司机驾驶行为分析记录")

            # 生成一些统计报告
            print("\n司机驾驶行为统计概要:")
            print(f"司机总数: {len(df)}")
            print(f"平均行程时长: {df['avg_trip_duration'].mean():.2f} 秒")
            print(f"平均早晨行程数: {df['morning_trips'].mean():.2f}")
            print(f"平均夜间行程数: {df['night_trips'].mean():.2f}")

    except Exception as e:
        print("分析司机行为时出错:", e)


if __name__ == "__main__":
    print("开始司机驾驶行为分析...")
    create_driver_analysis_table()
    analyze_driver_behavior()
    print("分析完成!")
