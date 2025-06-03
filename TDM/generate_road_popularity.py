from sqlalchemy import create_engine, text
import pandas as pd

# 数据库连接配置
username = 'yjy'
password = 'Yjy123456'
host = 'rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com'
port = '5432'
database = 'dataplatform'

engine = create_engine(
    f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',
    connect_args={'options': '-c client_encoding=UTF8'}
)

def create_popularity_table():
    try:
        with engine.begin() as conn:
            # 创建新表
            print("Creating target table...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.tdm_road_popularity_test (
                    way_id BIGINT PRIMARY KEY,
                    frequence INT,
                    popularity_tag VARCHAR(20)
                )
            """))
            conn.execute(text("TRUNCATE TABLE public.tdm_road_popularity_test"))
        print('Table created and truncated.')
    except Exception as e:
        print('Error in table creation:', e)

def calculate_and_write_popularity():
    try:
        # 1. 聚合所有way_id统计出现频率
        with engine.begin() as conn:
            # 利用unnest爆破road_list，统计所有way_id频次
            sql = """
                SELECT way_id, COUNT(*) as frequence
                FROM (
                    SELECT UNNEST(road_list) AS way_id
                    FROM public.dwd_trip_roads
                ) t
                GROUP BY way_id
            """
            df = pd.read_sql(sql, conn)
        print("Total unique way_id:", len(df))

        # 2. 按出现次数排序分层（可动态设定比例/分位数，这里示例四等分）
        df = df.sort_values('frequence', ascending=False).reset_index(drop=True)

        # 确定分层界限（四分位）
        quantiles = df['frequence'].quantile([0.75, 0.5, 0.25]).values
        q1, q2, q3 = quantiles  # 从大到小排序

        def tag_row(freq):
            if freq >= q1:
                return '极热门路段'
            elif freq >= q2:
                return '热门路段'
            elif freq >= q3:
                return '普通路段'
            else:
                return '冷门路段'

        df['popularity_tag'] = df['frequence'].apply(tag_row)

        # 3. 写入新表
        with engine.begin() as conn:
            # to_sql需设置if_exists和index
            df[['way_id','frequence','popularity_tag']].to_sql(
                'tdm_road_popularity_test',
                conn,
                schema='public',
                if_exists='append',
                index=False,
                method='multi'
            )
        print("Data written successfully.")

    except Exception as e:
        print('Error in calculating/writing:', e)

if __name__ == '__main__':
    create_popularity_table()
    calculate_and_write_popularity()
