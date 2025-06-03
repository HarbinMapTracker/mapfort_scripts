from sqlalchemy import create_engine, text

username = 'yjy'
password = 'Yjy123456'
host = 'rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com'
port = '5432'
database = 'dataplatform'

engine = create_engine(
    f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',
    connect_args={'options': '-c client_encoding=UTF8'}
)

def create_profile_table():
    try:
        print("Using DB:", engine.url)
        with engine.begin() as conn:
            print('Creating profile table...')
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.tdm_driver_trip_profile_test (
                    devid TEXT,
                    trip_count INT,
                    avg_travel_time FLOAT,
                    max_travel_time INT,
                    min_travel_time INT,
                    total_travel_time BIGINT
                )
            """))
            print('Truncating profile table...')
            conn.execute(text("TRUNCATE TABLE public.tdm_driver_trip_profile_test"))
        print('Done.')
    except Exception as e:
        print("Error:", e)

def analyze_driver_profile():
    try:
        with engine.begin() as conn:
            print("Aggregating driver statistics and inserting into profile table...")
            conn.execute(text("""
                INSERT INTO public.tdm_driver_trip_profile_test (devid, trip_count, avg_travel_time, max_travel_time, min_travel_time, total_travel_time)
                SELECT
                    devid,
                    COUNT(*) AS trip_count,
                    AVG(travel_time) AS avg_travel_time,
                    MAX(travel_time) AS max_travel_time,
                    MIN(travel_time) AS min_travel_time,
                    SUM(travel_time) AS total_travel_time
                FROM public.dwd_trip_info
                GROUP BY devid
            """))
            print("Profile analysis complete.")
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    create_profile_table()
    analyze_driver_profile()
