from sqlalchemy import create_engine, text
import pandas as pd
import logging

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


# 创建新表用于存储区域上车记录分析结果
def create_analysis_table():
    try:
        logging.info("Using DB: %s", engine.url)
        with engine.begin() as conn:
            logging.info('Creating table for district pickup analysis...')
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.district_pickup_analysis_test (
                    pac BIGINT PRIMARY KEY,
                    district_name TEXT,
                    city TEXT,
                    province TEXT,
                    pickup_count BIGINT
                )
            """))
            logging.info('Truncating table...')
            conn.execute(text("TRUNCATE TABLE public.district_pickup_analysis_test"))
        logging.info('Table creation done.')
    except Exception as e:
        logging.error("Error during table creation: %s", e)


# 检查数据范围和有效性
def check_data_validity():
    try:
        logging.info("Checking data validity for dwd_pickup_points...")
        query = """
            SELECT 
                MIN(lon) AS min_lon, MAX(lon) AS max_lon,
                MIN(lat) AS min_lat, MAX(lat) AS max_lat,
                COUNT(*) AS total_points
            FROM public.dwd_pickup_points
        """
        df = pd.read_sql(query, engine)
        logging.info("Pickup points stats: %s", df.to_dict())

        # 检查 harbin_districts 的边界范围
        boundary_query = """
            SELECT 
                ST_XMin(ST_Envelope(ST_Union(geometry))) AS min_lon,
                ST_XMax(ST_Envelope(ST_Union(geometry))) AS max_lon,
                ST_YMin(ST_Envelope(ST_Union(geometry))) AS min_lat,
                ST_YMax(ST_Envelope(ST_Union(geometry))) AS max_lat
            FROM public.harbin_districts
        """
        boundary_df = pd.read_sql(boundary_query, engine)
        logging.info("Harbin districts boundary stats: %s", boundary_df.to_dict())
    except Exception as e:
        logging.error("Error during data validity check: %s", e)


# 从 harbin_districts 和 dwd_pickup_points 表中提取数据并分析
def analyze_pickup_by_district():
    try:
        # 使用 ST_DWithin 替代 ST_Within，增加容差范围（例如 100 米）
        query = """
            SELECT 
                d."PAC" AS pac,
                d."NAME" AS district_name,
                d."市" AS city,
                d."省" AS province,
                COUNT(p.traj_id) AS pickup_count
            FROM 
                public.harbin_districts d
            LEFT JOIN 
                public.dwd_pickup_points p
            ON 
                ST_DWithin(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), d.geometry, 0.001)
            GROUP BY 
                d."PAC", d."NAME", d."市", d."省"
        """
        # 使用 pandas 读取查询结果
        df = pd.read_sql(query, engine)
        logging.info("Data extraction and analysis completed. Total rows: %d", len(df))

        # 检查未匹配的点
        unmatched_query = """
            SELECT COUNT(*) AS unmatched_count
            FROM public.dwd_pickup_points p
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.harbin_districts d
                WHERE ST_DWithin(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), d.geometry, 0.001)
            )
        """
        unmatched_df = pd.read_sql(unmatched_query, engine)
        logging.info("Unmatched pickup points: %s", unmatched_df.to_dict())

        return df
    except Exception as e:
        logging.error("Error during data analysis: %s", e)
        return None


# 将分析结果写入新表
def write_to_table(df):
    try:
        if df is not None and not df.empty:
            with engine.begin() as conn:
                logging.info("Writing analysis results to table...")
                df.to_sql('tdm_district_pickup_analysis_test', conn, schema='public', if_exists='append', index=False)
                logging.info("Data successfully written to table.")
        else:
            logging.warning("No data to write to table.")
    except Exception as e:
        logging.error("Error during data writing: %s", e)


# 创建空间索引以加速查询
def create_spatial_index():
    try:
        with engine.begin() as conn:
            logging.info("Creating spatial index if not exists...")
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_harbin_districts_geometry ON public.harbin_districts USING GIST(geometry);
            """))
            logging.info("Spatial index creation done.")
    except Exception as e:
        logging.error("Error during spatial index creation: %s", e)


# 主函数，执行完整流程
def main():
    # 创建空间索引
    create_spatial_index()

    # 检查数据有效性
    check_data_validity()

    # 创建新表
    create_analysis_table()

    # 分析区域上车记录
    analysis_df = analyze_pickup_by_district()

    # 将结果写入新表
    write_to_table(analysis_df)


if __name__ == "__main__":
    main()
