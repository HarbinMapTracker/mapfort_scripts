from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.sql import text

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

# 创建表的函数
def create_table():
    try:
        print("Using DB:", engine.url)
        with engine.begin() as conn:
            print('Creating table...')
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.tdm_areas_roads (
                    node_id BIGINT PRIMARY KEY,
                    devid TEXT,
                    lon FLOAT,
                    lat FLOAT,
                    district_code VARCHAR(12),
                    district_name VARCHAR(12),
                    way_id BIGINT
                )
            """))
            print('Truncating table...')
            conn.execute(text("TRUNCATE TABLE public.tdm_areas_roads"))
        print('Done.')
    except Exception as e:
        print("Error:", e)

# 插入数据的函数
def insert_data():
    try:
        print("Inserting data into tdm_areas_roads...")
        with engine.begin() as conn:
            # 使用空间查询，将上车点与行政区进行关联
            # 同时关联 dwd_trip_roads 获取行程相关信息
            insert_query = text("""
                INSERT INTO public.tdm_areas_roads (node_id, devid, lon, lat, district_code,district_name, way_id)
                SELECT 
                    p.traj_id AS node_id,
                    p.devid,
                    p.lon,
                    p.lat,
                    h."PAC" AS district_code,
                    h."NAME" As district_name,
                    p.way_id
                FROM public.dwd_pickup_points p
                LEFT JOIN public.harbin_districts h
                ON ST_Within(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), h.geometry)
            """)
            conn.execute(insert_query)
        print("Data insertion completed.")
    except Exception as e:
        print("Error during data insertion:", e)

# 主函数
def main():
    # 创建表
    create_table()
    # 插入数据
    insert_data()

if __name__ == "__main__":
    main()
