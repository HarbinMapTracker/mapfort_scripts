import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import HSTORE
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    source_engine = create_engine(
        'postgresql+psycopg2://osmuser:pass@106.75.3.204:5432/harbin',
        connect_args={'options': '-c client_encoding=UTF8'}
    )
    with source_engine.connect() as conn:
        logging.info("Source database (harbin) connected successfully")
except Exception as e:
    logging.error(f"Failed to connect to source database: {e}")
    raise

try:
    target_engine = create_engine(
        'postgresql+psycopg2://yjy:Yjy123456@rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com:5432/dataplatform',
        connect_args={'options': '-c client_encoding=UTF8'},
        pool_pre_ping=True
    )
    with target_engine.connect() as conn:
        logging.info("Target Aliyun RDS database (dataplatform) connected successfully")
except Exception as e:
    logging.error(f"Failed to connect to target database: {e}")
    raise

with target_engine.connect() as conn:
    try:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS hstore;"))
        conn.execute(text("COMMIT;"))
        logging.info("Hstore extension enabled")
    except Exception as e:
        logging.error(f"Failed to enable hstore extension: {e}")
        conn.execute(text("ROLLBACK;"))
        raise

logging.info("Reading relations...")
relations_df = pd.read_sql(
    "SELECT id, tags FROM relations",
    source_engine
)
logging.info(f"Relations loaded: {len(relations_df)} rows")
logging.info(f"Sample relations: {relations_df.head().to_dict(orient='records')}")

logging.info("Reading relation_members...")
relation_members_df = pd.read_sql(
    "SELECT relation_id, member_id, member_type, sequence_id FROM relation_members",
    source_engine
)
logging.info(f"Relation_members loaded: {len(relation_members_df)} rows")
logging.info(f"Sample relation_members: {relation_members_df.head().to_dict(orient='records')}")

common_ids = set(relation_members_df['relation_id']).intersection(set(relations_df['id']))
logging.info(f"Common relation_id count: {len(common_ids)}")
logging.info(f"relation_members relation_id count: {len(set(relation_members_df['relation_id']))}")
logging.info(f"relations id count: {len(set(relations_df['id']))}")

logging.info("Merging relation_members and relations...")
areas_df = relation_members_df.merge(
    relations_df[['id', 'tags']],
    left_on='relation_id',
    right_on='id',
    how='left'
)
areas_df = areas_df.drop(columns=['id'])

unmatched = areas_df[areas_df['tags'].isna()]
logging.info(f"Unmatched relation_id count: {len(unmatched)}")
if len(unmatched) > 0:
    logging.info(f"Sample unmatched relation_id: {unmatched['relation_id'].head().tolist()}")

def clean_hstore_tags(tags):
    if not isinstance(tags, dict) or pd.isna(tags):
        logging.debug(f"Empty or NaN tags: {tags}")
        return {}
    try:
        return {str(k): str(v)[:255] for k, v in tags.items() if v is not None}
    except Exception as e:
        logging.warning(f"Invalid tags value: {tags}, error: {e}")
        return {}

areas_df['tags_raw'] = areas_df['tags']
areas_df['tags'] = areas_df['tags'].apply(clean_hstore_tags)
logging.info(f"Empty tags count: {len(areas_df[areas_df['tags'].apply(len) == 0])}")
logging.info(f"Sample raw tags: {areas_df['tags_raw'].head().tolist()}")
logging.info(f"Sample cleaned tags: {areas_df['tags'].head().tolist()}")

areas_df = areas_df.rename(columns={'member_id': 'id'})
areas_df = areas_df[['id', 'relation_id', 'member_type', 'sequence_id', 'tags']]

logging.info(f"dwd_areas cleaned: {len(areas_df)} rows")
logging.info("areas_df sample:")
logging.info(areas_df.head().to_dict(orient='records'))
logging.info("areas_df columns: %s", areas_df.columns.tolist())
logging.info("areas_df null counts:\n%s", areas_df.isna().sum().to_dict())
logging.info("tags column types:\n%s", areas_df['tags'].apply(type).value_counts())
logging.info("member_type distribution:\n%s", areas_df['member_type'].value_counts().to_dict())
duplicate_ids = areas_df[areas_df.duplicated(subset=['id'], keep=False)]
logging.info(f"Duplicate id count: {len(duplicate_ids)}")

# 创建 dwd_areas 表
create_areas_query = """
CREATE TABLE IF NOT EXISTS public.dwd_areas (
    id BIGINT,
    relation_id BIGINT,
    member_type TEXT,
    sequence_id INTEGER,
    tags HSTORE
);
"""
with target_engine.connect() as conn:
    try:
        logging.info("Dropping dwd_areas table...")
        conn.execute(text("DROP TABLE IF EXISTS public.dwd_areas CASCADE;"))
        table_exists = conn.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'dwd_areas')"
        )).fetchone()[0]
        logging.info(f"dwd_areas exists after drop: {table_exists}")
        logging.info("Creating dwd_areas table...")
        conn.execute(text(create_areas_query))
        table_check = conn.execute(text(
            "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'dwd_areas'"
        )).fetchall()
        columns = [(col[0], col[1]) for col in table_check]
        logging.info("dwd_areas columns (name, nullable): %s", columns)
        if 'id' not in [col[0] for col in columns]:
            raise ValueError("id column not found in dwd_areas table")
        conn.execute(text("COMMIT;"))
    except Exception as e:
        logging.error(f"Failed to create dwd_areas table: {e}")
        conn.execute(text("ROLLBACK;"))
        raise

try:
    with target_engine.connect() as conn:
        conn.execute(text("ROLLBACK;"))
        logging.info("Table state before insert:")
        count_before = conn.execute(text("SELECT COUNT(*) FROM public.dwd_areas")).fetchone()[0]
        logging.info(f"dwd_areas rows before insert: {count_before}")
        logging.info("Inserting data into dwd_areas...")
        areas_df.to_sql(
            'dwd_areas',
            target_engine,
            schema='public',
            if_exists='append',
            index=False,
            dtype={'tags': HSTORE},
            chunksize=500,
            method='multi'
        )
        conn.execute(text("COMMIT;"))
        logging.info(f"dwd_areas written: {len(areas_df)} rows")
        with target_engine.connect() as new_conn:
            count_after = new_conn.execute(text("SELECT COUNT(*) FROM public.dwd_areas")).fetchone()[0]
            logging.info(f"dwd_areas rows after insert: {count_after}")
            if count_after != len(areas_df):
                raise ValueError(f"Insert failed: expected {len(areas_df)} rows, got {count_after}")
            sample = new_conn.execute(text("SELECT * FROM public.dwd_areas LIMIT 5")).fetchall()
            logging.info(f"dwd_areas sample rows: {sample}")
            non_empty_tags = new_conn.execute(text(
                "SELECT COUNT(*) FROM public.dwd_areas WHERE tags != ''::hstore"
            )).fetchone()[0]
            logging.info(f"dwd_areas non-empty tags count: {non_empty_tags}")
            sample_tags = new_conn.execute(text(
                "SELECT tags->'name' AS name FROM public.dwd_areas WHERE tags ? 'name' LIMIT 5"
            )).fetchall()
            logging.info(f"dwd_areas sample tags->name: {sample_tags}")
            tags_keys = new_conn.execute(text(
                "SELECT akeys(tags) AS keys FROM public.dwd_areas WHERE tags != ''::hstore LIMIT 5"
            )).fetchall()
            logging.info(f"dwd_areas sample tags keys: {[row[0] for row in tags_keys]}")
            name_count = new_conn.execute(text(
                "SELECT COUNT(*) FROM public.dwd_areas WHERE tags ? 'name'"
            )).fetchone()[0]
            logging.info(f"dwd_areas tags with 'name' count: {name_count}")
            type_count = new_conn.execute(text(
                "SELECT COUNT(*) FROM public.dwd_areas WHERE tags ? 'type'"
            )).fetchone()[0]
            logging.info(f"dwd_areas tags with 'type' count: {type_count}")
except Exception as e:
    logging.error(f"Failed to insert data: {e}")
    with target_engine.connect() as conn:
        conn.execute(text("ROLLBACK;"))
    raise

output_dir = os.path.expanduser('~/DataPlatform/process/DWD')
os.makedirs(output_dir, exist_ok=True)
areas_df.to_parquet(os.path.join(output_dir, 'areas.parquet'), index=False, engine='pyarrow')
logging.info(f"Parquet file saved to {output_dir}/areas.parquet")

parquet_df = pd.read_parquet(os.path.join(output_dir, 'areas.parquet'))
logging.info(f"Parquet file rows: {len(parquet_df)}")
logging.info(f"Parquet non-empty tags count: {len(parquet_df[parquet_df['tags'].apply(len) > 0])}")
logging.info(f"Parquet sample tags: {parquet_df['tags'].head().tolist()}")

logging.info("Map layer table (dwd_areas) created successfully in Aliyun RDS!")