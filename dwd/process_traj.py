import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import logging
import json
import psycopg2
from psycopg2.extras import execute_values
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

db_params = {
    'dbname': 'dataplatform',
    'user': 'yjy',
    'password': 'Yjy123456',
    'host': 'rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com',
    'port': '5432',
    'options': '-c client_encoding=UTF8',
    'connect_timeout': 60
}

input_file = '/home/users/yejinyan/DataPlatform/process/ODS2/trips_full.parquet'

pickup_table = 'public.pickup_points'
trip_info_table = 'public.trip_info'
trip_roads_table = 'public.trip_roads'

approx_rows_per_chunk = 100000


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
        cur.execute("SET idle_in_transaction_session_timeout = 0;")
        cur.execute(f"""
            DROP TABLE IF EXISTS {pickup_table}, {trip_info_table}, {trip_roads_table} CASCADE;
        """)

        cur.execute("""
            CREATE TABLE public.pickup_points (
                traj_id SERIAL PRIMARY KEY,
                devid TEXT,
                lon FLOAT,
                lat FLOAT,
                tms BIGINT,
                way_id BIGINT
            );
        """)
        cur.execute("""
            CREATE TABLE public.trip_info (
                traj_id SERIAL PRIMARY KEY,
                devid TEXT,
                travel_time BIGINT,
                begin_time BIGINT,
                end_time BIGINT
            );
        """)
        cur.execute("""
            CREATE TABLE public.trip_roads (
                traj_id SERIAL PRIMARY KEY,
                devid TEXT,
                road_list BIGINT[],
                tms_list BIGINT[]
            );
        """)
        conn.commit()
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_name IN ('pickup_points', 'trip_info', 'trip_roads');
        """)
        tables = cur.fetchall()
        logging.info(f"Compete table: {tables}")


def validate_row(row, index):
    errors = []
    fields = ['lon', 'lat', 'tms', 'roads', 'route']
    parsed_row = {}

    for field in fields:
        value = row[field]
        try:
            if isinstance(value, np.ndarray):
                value = value.tolist()
            elif isinstance(value, str):
                value = json.loads(value)
            parsed_row[field] = value

            if not isinstance(parsed_row[field], list):
                errors.append(f"行 {index}: {field} 不是列表类型，实际值: {value}")
            elif not parsed_row[field] and field not in ['roads', 'route']:
                errors.append(f"行 {index}: {field} 是空列表")
        except (json.JSONDecodeError, TypeError):
            errors.append(f"行 {index}: {field} 解析失败，实际值: {value}")
            parsed_row[field] = []

    if index < 5:
        logging.info(f"行 {index} 样本数据: {dict(row)}")
        logging.info(f"行 {index} 解析后数据: {parsed_row}")

    return parsed_row, errors


def process_chunk(chunk, start_traj_id):
    pickup_data = []
    trip_info_data = []
    trip_roads_data = []
    traj_id = start_traj_id
    empty_roads_count = 0
    empty_roads_devids = []
    invalid_way_id_count = 0
    empty_tms_list_count = 0

    for index, row in chunk.iterrows():
        parsed_row, errors = validate_row(row, index)
        if errors:
            for error in errors:
                logging.warning(error)

        if not parsed_row['roads']:
            empty_roads_count += 1
            empty_roads_devids.append(str(row['devid']))
            logging.info(f"行 {index}: 跳过空 roads 记录，devid={row['devid']}")
            if parsed_row['route']:
                logging.warning(
                    f"行 {index}: roads 为空但 route 非空，devid={row['devid']}, route={parsed_row['route']}")

        if isinstance(parsed_row['tms'], list) and len(parsed_row['tms']) > 1:
            try:
                begin_time = int(float(parsed_row['tms'][0])) if pd.notnull(
                    parsed_row['tms'][0]) else None
                end_time = int(
                    float(parsed_row['tms'][-1])) if pd.notnull(parsed_row['tms'][-1]) else None
                travel_time = end_time - \
                    begin_time if begin_time is not None and end_time is not None else 0
            except (ValueError, TypeError) as e:
                logging.warning(
                    f"行 {index}: travel_time 计算失败，tms={parsed_row['tms']}, 错误: {str(e)}")
                begin_time = None
                end_time = None
                travel_time = 0
        else:
            logging.info(f"行 {index}: tms 无效，tms={parsed_row['tms']}")
            begin_time = None
            end_time = None
            travel_time = 0
        trip_info_data.append((
            traj_id,
            str(row['devid']) if pd.notnull(row['devid']) else None,
            travel_time,
            begin_time,
            end_time
        ))

        # pickup_points 和 trip_roads 表数据（仅当 roads 非空时保存）
        if parsed_row['roads']:
            lon = parsed_row['lon'][0] if isinstance(
                parsed_row['lon'], list) and parsed_row['lon'] else None
            lat = parsed_row['lat'][0] if isinstance(
                parsed_row['lat'], list) and parsed_row['lat'] else None
            tms = parsed_row['tms'][0] if isinstance(
                parsed_row['tms'], list) and parsed_row['tms'] else None
            way_id = parsed_row['route'][0] if parsed_row['route'] and pd.notnull(
                parsed_row['route'][0]) else None
            if way_id is not None:
                try:
                    way_id = int(float(way_id))
                    if way_id < -2**63 or way_id > 2**63 - 1:
                        raise ValueError("way_id out of BIGINT range")
                except (ValueError, TypeError) as e:
                    logging.error(
                        f"行 {index}: 无效 way_id，值: {way_id}, 错误: {str(e)}")
                    way_id = None
                    invalid_way_id_count += 1
            pickup_data.append((
                traj_id,
                str(row['devid']) if pd.notnull(row['devid']) else None,
                float(lon) if lon is not None else None,
                float(lat) if lat is not None else None,
                int(float(tms)) if tms is not None else None,
                way_id
            ))

            try:
                road_list = [int(float(x)) for x in parsed_row['roads'] if pd.notnull(
                    x) and isinstance(x, (int, float))]
                tms_list = [int(float(x)) for x in parsed_row['tms'] if pd.notnull(
                    x) and isinstance(x, (int, float))]
                if not road_list:
                    empty_roads_count += 1
                    empty_roads_devids.append(str(row['devid']))
                    logging.info(
                        f"行 {index}: road_list 为空，devid={row['devid']}")
                if not tms_list:
                    empty_tms_list_count += 1
                    logging.warning(
                        f"行 {index}: tms_list 为空，tms={parsed_row['tms']}, devid={row['devid']}")
            except (ValueError, TypeError) as e:
                logging.error(
                    f"行 {index}: road_list 或 tms_list 转换失败，roads={parsed_row['roads']}, tms={parsed_row['tms']}, 错误: {str(e)}")
                road_list = []
                tms_list = []
                empty_roads_count += 1
                empty_roads_devids.append(str(row['devid']))
            if road_list:
                trip_roads_data.append((
                    traj_id,
                    str(row['devid']) if pd.notnull(row['devid']) else None,
                    road_list,
                    tms_list
                ))

        traj_id += 1

    pickup_df = pd.DataFrame(pickup_data, columns=[
                             'traj_id', 'devid', 'lon', 'lat', 'tms', 'way_id'])
    trip_info_df = pd.DataFrame(trip_info_data, columns=[
                                'traj_id', 'devid', 'travel_time', 'begin_time', 'end_time'])
    trip_roads_df = pd.DataFrame(trip_roads_data, columns=[
                                 'traj_id', 'devid', 'road_list', 'tms_list'])

    if not pickup_df.empty:
        logging.info(
            f"pickup_df 样本（前 5 行）：{pickup_df.head().to_dict(orient='records')}")
    if not trip_info_df.empty:
        logging.info(
            f"trip_info_df 样本（前 5 行）：{trip_info_df.head().to_dict(orient='records')}")
    if not trip_roads_df.empty:
        logging.info(
            f"trip_roads_df 样本（前 5 行）：{trip_roads_df.head().to_dict(orient='records')}")

    return pickup_df, trip_info_df, trip_roads_df, traj_id, empty_roads_count, empty_roads_devids, invalid_way_id_count, empty_tms_list_count


def write_to_db(df, table_name, conn, attempt, block_index):
    try:
        if not df.empty:
            with conn.cursor() as cur:
                if table_name == pickup_table:
                    insert_query = """
                        INSERT INTO public.pickup_points (traj_id, devid, lon, lat, tms, way_id)
                        VALUES %s
                    """
                    values = [tuple(row) for row in df[[
                        'traj_id', 'devid', 'lon', 'lat', 'tms', 'way_id']].values]
                elif table_name == trip_info_table:
                    insert_query = """
                        INSERT INTO public.trip_info (traj_id, devid, travel_time, begin_time, end_time)
                        VALUES %s
                    """
                    values = [tuple(row) for row in df[[
                        'traj_id', 'devid', 'travel_time', 'begin_time', 'end_time']].values]
                elif table_name == trip_roads_table:
                    insert_query = """
                        INSERT INTO public.trip_roads (traj_id, devid, road_list, tms_list)
                        VALUES %s
                    """
                    values = [
                        tuple(row) for row in df[['traj_id', 'devid', 'road_list', 'tms_list']].values]

                execute_values(cur, insert_query, values, page_size=1000)
                conn.commit()

                # 验证写入
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                logging.info(
                    f"成功写入 {len(df)} 条记录到 {table_name}，当前总记录数: {count}")
                if count == 0:
                    logging.warning(f"写入 {table_name} 后记录数为 0，可能事务未提交或被回滚")
        else:
            logging.info(f"{table_name} 数据为空，跳过写入")
        return True
    except psycopg2.Error as e:
        logging.error(
            f"写入 {table_name} 失败 (尝试 {attempt+1}/3, 块 {block_index}): {str(e)}")
        logging.error(
            f"失败的 DataFrame 样本: {df.head().to_dict(orient='records')}")
        df.to_csv(
            f"failed_{table_name}_block_{block_index}_{attempt}.csv", index=False)
        conn.rollback()
        return False
    except Exception as e:
        logging.error(
            f"写入 {table_name} 时发生未知错误 (尝试 {attempt+1}/3, 块 {block_index}): {str(e)}")
        conn.rollback()
        return False


def main():
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public;")
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_name IN ('pickup_points', 'trip_info', 'trip_roads');
            """)
            tables = cur.fetchall()
            logging.info(f"程序启动前数据库表: {tables}")
            cur.execute("""
                SELECT * 
                FROM information_schema.triggers 
                WHERE event_object_table IN ('pickup_points', 'trip_info', 'trip_roads');
            """)
            triggers = cur.fetchall()
            logging.info(f"数据库触发器: {triggers}")
            cur.execute("""
                SELECT grantee, privilege_type 
                FROM information_schema.table_privileges 
                WHERE table_name IN ('pickup_points', 'trip_info', 'trip_roads') AND grantee = 'yjy';
            """)
            permissions = cur.fetchall()
            logging.info(f"用户 yjy 权限: {permissions}")

        create_tables(conn)

        total_rows = 0
        total_empty_roads = 0
        total_invalid_way_id = 0
        total_empty_tms_list = 0
        empty_roads_devids = []
        current_traj_id = 1

        parquet_file = pq.ParquetFile(input_file)
        num_row_groups = parquet_file.num_row_groups
        total_rows_est = parquet_file.metadata.num_rows
        logging.info(f"Parquet 文件包含 {total_rows_est} 条记录，{num_row_groups} 个行组")

        schema = parquet_file.schema_arrow
        columns = [field.name for field in schema]
        logging.info(f"Parquet 文件列名: {columns}")

        expected_columns = ['lon', 'lat', 'tms', 'roads', 'route', 'devid']
        missing_columns = [
            col for col in expected_columns if col not in columns]
        if missing_columns:
            logging.error(f"缺少预期列: {missing_columns}")
            raise ValueError(f"Parquet 文件缺少列: {missing_columns}")

        rows_per_group = total_rows_est // num_row_groups if num_row_groups > 0 else 1
        groups_per_chunk = max(1, approx_rows_per_chunk // rows_per_group)

        for i in range(0, num_row_groups, groups_per_chunk):
            end_group = min(i + groups_per_chunk, num_row_groups)
            try:
                chunk = parquet_file.read_row_groups(
                    range(i, end_group)).to_pandas()
                logging.info(f"正在处理 {len(chunk)} 条记录 (行组 {i} 到 {end_group-1})")
            except Exception as e:
                logging.error(f"读取行组 {i} 到 {end_group-1} 失败: {str(e)}")
                continue

            try:
                pickup_df, trip_info_df, trip_roads_df, current_traj_id, empty_roads_count, chunk_empty_roads_devids, invalid_way_id_count, empty_tms_list_count = process_chunk(
                    chunk, current_traj_id)
                total_empty_roads += empty_roads_count
                total_invalid_way_id += invalid_way_id_count
                total_empty_tms_list += empty_tms_list_count
                empty_roads_devids.extend(chunk_empty_roads_devids)
                logging.info(
                    f"处理完成 {len(chunk)} 条记录,traj_id 范围: {current_traj_id-len(chunk)} 到 {current_traj_id-1}, 本块空 roads 记录: {empty_roads_count}, 无效 way_id 记录: {invalid_way_id_count}, 空 tms_list 记录: {empty_tms_list_count}")
                logging.info(
                    f"本块空 roads devid 样本:{chunk_empty_roads_devids[:5]}")
                total_rows += len(chunk)
            except Exception as e:
                logging.error(f"处理数据块 {i} 失败: {str(e)}")
                continue

            for attempt in range(3):
                try:
                    with conn.cursor() as cur:
                        cur.execute("SET search_path TO public;")
                        cur.execute("SELECT txid_current()")
                        txid_before = cur.fetchone()[0]
                        logging.info(f"写入块 {i} 开始，事务 ID: {txid_before}")

                        success_pickup = write_to_db(
                            pickup_df, pickup_table, conn, attempt, i)
                        success_trip_info = write_to_db(
                            trip_info_df, trip_info_table, conn, attempt, i)
                        success_trip_roads = write_to_db(
                            trip_roads_df, trip_roads_table, conn, attempt, i)

                        cur.execute(f"SELECT COUNT(*) FROM {pickup_table}")
                        pickup_count = cur.fetchone()[0]
                        cur.execute(f"SELECT COUNT(*) FROM {trip_info_table}")
                        trip_info_count = cur.fetchone()[0]
                        cur.execute(f"SELECT COUNT(*) FROM {trip_roads_table}")
                        trip_roads_count = cur.fetchone()[0]
                        logging.info(
                            f"写入后记录数：pickup_points={pickup_count}, trip_info={trip_info_count}, trip_roads={trip_roads_count}")

                        cur.execute("SELECT txid_current()")
                        txid_after = cur.fetchone()[0]
                        logging.info(
                            f"写入块 {i} 完成，事务 ID: 写入前={txid_before}, 写入后={txid_after}")
                        logging.info(
                            f"成功写入 {len(chunk)} 条记录到数据库（pickup_points={len(pickup_df)}, trip_info={len(trip_info_df)}, trip_roads={len(trip_roads_df)}）")
                        break
                except psycopg2.Error as e:
                    logging.warning(
                        f"数据库写入失败 (块 {i}, 尝试 {attempt+1}/3): {str(e)}")
                    conn.rollback()
                    if attempt == 2:
                        logging.error(f"数据库写入最终失败 (块 {i}): {str(e)}")
                        continue
                    time.sleep(2)
                except Exception as e:
                    logging.error(
                        f"写入过程中发生未知错误 (块 {i}, 尝试 {attempt+1}/3): {str(e)}")
                    # 保存失败的 DataFrame
                    pickup_df.to_csv(
                        f"failed_pickup_points_block_{i}_{attempt}.csv", index=False)
                    trip_info_df.to_csv(
                        f"failed_trip_info_block_{i}_{attempt}.csv", index=False)
                    trip_roads_df.to_csv(
                        f"failed_trip_roads_block_{i}_{attempt}.csv", index=False)
                    conn.rollback()
                    break

        logging.info(
            f"处理完成，总共处理 {total_rows} 条记录，空 roads 记录总数: {total_empty_roads}, 无效 way_id 记录总数: {total_invalid_way_id}, 空 tms_list 记录总数: {total_empty_tms_list}")
        logging.info(f"空 roads devid 样本（前 10）：{empty_roads_devids[:10]}")

        with conn.cursor() as cur:
            cur.execute("SET search_path TO public;")
            cur.execute(f"SELECT COUNT(*) FROM {pickup_table}")
            pickup_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {trip_info_table}")
            trip_info_count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {trip_roads_table}")
            trip_roads_count = cur.fetchone()[0]
            logging.info(
                f"数据库记录数：pickup_points={pickup_count}, trip_info={trip_info_count}, trip_roads={trip_roads_count}")

            cur.execute(f"SELECT * FROM {pickup_table} LIMIT 5")
            pickup_sample = cur.fetchall()
            cur.execute(f"SELECT * FROM {trip_info_table} LIMIT 5")
            trip_info_sample = cur.fetchall()
            cur.execute(f"SELECT * FROM {trip_roads_table} LIMIT 5")
            trip_roads_sample = cur.fetchall()
            logging.info(f"pickup_points 样本: {pickup_sample}")
            logging.info(f"trip_info 样本: {trip_info_sample}")
            logging.info(f"trip_roads 样本: {trip_roads_sample}")

            cur.execute("""
                SELECT * 
                FROM pg_stat_activity 
                WHERE state = 'idle in transaction' AND datname = 'dataplatform';
            """)
            activity = cur.fetchall()
            if activity:
                logging.warning(f"检测到未关闭事务: {activity}")

    except Exception as e:
        logging.error(f"处理失败: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("数据库连接已关闭")


if __name__ == "__main__":
    main()
