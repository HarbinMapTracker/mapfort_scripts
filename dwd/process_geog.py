import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import HSTORE, insert
import numpy as np
from shapely import wkb
import os
from geoalchemy2 import Geometry, WKBElement
from shapely.wkt import dumps as wkt_dumps

def parse_geom(geom):
    try:
        if geom is None:
            return None
        if isinstance(geom, WKBElement):
            return wkb.loads(geom.data)
        return wkb.loads(geom)
    except Exception as e:
        print(f"Failed to parse geom: {e}")
        return None

try:
    source_engine = create_engine(
        'postgresql+psycopg2://osmuser:pass@106.75.3.204:5432/harbin',
        connect_args={'options': '-c client_encoding=UTF8'}
    )
    with source_engine.connect() as conn:
        print("Source database connected successfully")
except Exception as e:
    print(f"Failed to connect to source database: {e}")
    raise

try:
    target_engine = create_engine(
        'postgresql+psycopg2://yjy:Yjy123456@rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com:5432/dataplatform',
        connect_args={'options': '-c client_encoding=UTF8'}
    )
    with target_engine.connect() as conn:
        print("Target Aliyun RDS database connected successfully")
except Exception as e:
    print(f"Failed to connect to target database: {e}")
    raise

with target_engine.connect() as conn:
    try:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS hstore;"))
        print("PostGIS and hstore extensions enabled")
    except Exception as e:
        print(f"Failed to enable extensions: {e}")
        raise

print("Reading nodes...")
nodes_df = pd.read_sql(
    'SELECT id, geom FROM nodes WHERE ST_X(geom) BETWEEN 125.5 AND 127.5 AND ST_Y(geom) BETWEEN 44.5 AND 46.5',
    source_engine
)
print(f"Nodes loaded: {len(nodes_df)} rows")

print("Reading way_nodes...")
way_nodes_df = pd.read_sql(
    'SELECT node_id, way_id, sequence_id FROM way_nodes',
    source_engine
)
print(f"Way_nodes loaded: {len(way_nodes_df)} rows")

print("Reading bfmap_ways...")
bfmap_ways_df = pd.read_sql(
    'SELECT gid, osm_id, class_id, source, target, length, reverse, priority, geom FROM bfmap_ways',
    source_engine
)
print(f"Bfmap_ways loaded: {len(bfmap_ways_df)} rows")

print("Reading ways tags...")
ways_tags_df = pd.read_sql(
    "SELECT id, tags FROM ways WHERE tags ? 'name'",
    source_engine
)
print(f"Ways tags loaded: {len(ways_tags_df)} rows")

# 点级表 (dwd_points)
nodes_df['geom'] = nodes_df['geom'].apply(parse_geom)
nodes_df['lon'] = nodes_df['geom'].apply(lambda x: x.x if x else None)
nodes_df['lat'] = nodes_df['geom'].apply(lambda x: x.y if x else None)
nodes_df['geom'] = nodes_df['geom'].apply(
    lambda x: WKBElement(wkb.dumps(x), srid=4326) if x else None
)
nodes_df = nodes_df.rename(columns={'id': 'node_id'})

way_nodes_counts = way_nodes_df.groupby('node_id').size().reset_index(name='count')
single_node_ids = way_nodes_counts[way_nodes_counts['count'] == 1]['node_id']
way_nodes_single = way_nodes_df[way_nodes_df['node_id'].isin(single_node_ids)]

print(f"way_nodes with single match: {len(way_nodes_single)} rows")

points_df = nodes_df.merge(
    way_nodes_single,
    on='node_id',
    how='inner'
)
points_df = points_df[['node_id', 'lon', 'lat', 'geom', 'way_id', 'sequence_id']]
points_df = points_df.dropna(subset=['lon', 'lat', 'geom'])
print(f"dwd_points cleaned: {len(points_df)} rows")

print("points_df sample:")
print(points_df.head())
print("points_df columns:", points_df.columns.tolist())
print("points_df way_id null count:", points_df['way_id'].isna().sum())
print("points_df sequence_id null count:", points_df['sequence_id'].isna().sum())

create_points_query = '''
CREATE TABLE IF NOT EXISTS dwd_points (
    node_id BIGINT,
    lon FLOAT,
    lat FLOAT,
    geom GEOMETRY(Point, 4326),
    way_id BIGINT,
    sequence_id INTEGER,
    PRIMARY KEY (node_id, way_id)
);
'''
with target_engine.connect() as conn:
    try:
        print("Dropping dwd_points table...")
        conn.execute(text("DROP TABLE IF EXISTS dwd_points CASCADE;"))
        table_exists = conn.execute(text(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dwd_points')"
        )).fetchone()[0]
        print(f"dwd_points exists after drop: {table_exists}")
        print("Creating dwd_points table...")
        conn.execute(text(create_points_query))
        table_check = conn.execute(text(
            "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'dwd_points'"
        )).fetchall()
        columns = [(col[0], col[1]) for col in table_check]
        print("dwd_points columns (name, nullable):", columns)
        if 'way_id' not in [col[0] for col in columns]:
            raise ValueError("way_id column not found in dwd_points table")
        index_check = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'dwd_points' AND indexname = 'idx_dwd_points_geom'"
        )).fetchone()
        if not index_check:
            print("Creating spatial index for dwd_points...")
            conn.execute(text("CREATE INDEX idx_dwd_points_geom ON dwd_points USING GIST (geom);"))
        conn.execute(text("COMMIT;"))
    except Exception as e:
        print(f"Failed to create dwd_points table: {e}")
        raise

points_df['way_id'] = points_df['way_id'].fillna(-1)
points_df['sequence_id'] = points_df['sequence_id'].fillna(-1)

points_df.to_sql(
    'dwd_points',
    target_engine,
    if_exists='append',
    index=False,
    dtype={'geom': Geometry('POINT', srid=4326)},
    chunksize=2000,
    method='multi'
)
print(f"dwd_points written: {len(points_df)} rows")


# 道路级表 (dwd_roads)
roads_df = bfmap_ways_df.merge(
    ways_tags_df[['id', 'tags']],
    left_on='osm_id',
    right_on='id',
    how='left'
)
roads_df['tags'] = roads_df['tags'].apply(lambda x: x if isinstance(x, dict) else {})
roads_df['geom'] = roads_df['geom'].apply(
    lambda x: WKBElement(wkb.dumps(parse_geom(x)), srid=4326) if x else None
)
roads_df = roads_df[['osm_id', 'source', 'target', 'length', 'reverse', 'priority', 'class_id', 'geom', 'tags']].rename(
    columns={'osm_id': 'road_id'}
)
roads_df = roads_df.dropna(subset=['geom'])
print(f"dwd_roads cleaned: {len(roads_df)} rows")

print("tags column sample:")
print(roads_df['tags'].head())
print("tags column types:")
print(roads_df['tags'].apply(type).value_counts())

create_roads_query = """
CREATE TABLE IF NOT EXISTS dwd_roads (
    road_id BIGINT PRIMARY KEY,
    source BIGINT,
    target BIGINT,
    length FLOAT,
    reverse INTEGER,
    priority INTEGER,
    class_id INTEGER,
    geom GEOMETRY(LineString, 4326),
    tags HSTORE
);
"""
with target_engine.connect() as conn:
    try:
        print("Dropping dwd_roads table...")
        conn.execute(text("DROP TABLE IF EXISTS dwd_roads CASCADE;"))
        table_exists = conn.execute(text(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dwd_roads')"
        )).fetchone()[0]
        print(f"dwd_roads exists after drop: {table_exists}")
        print("Creating dwd_roads table...")
        conn.execute(text(create_roads_query))
        table_check = conn.execute(text(
            "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'dwd_roads'"
        )).fetchall()
        print("dwd_roads columns (name, nullable):", [(col[0], col[1]) for col in table_check])
        index_check = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'dwd_roads' AND indexname = 'idx_dwd_roads_geom'"
        )).fetchone()
        if not index_check:
            print("Creating spatial index for dwd_roads...")
            conn.execute(text("CREATE INDEX idx_dwd_roads_geom ON dwd_roads USING GIST (geom);"))
        conn.execute(text("COMMIT;"))
    except Exception as e:
        print(f"Failed to create dwd_roads table: {e}")
        raise

def insert_roads_with_conflict_ignore(df, table_name, engine):
    from sqlalchemy import MetaData, Table
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    records = df.to_dict('records')
    with engine.connect() as conn:
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['road_id'])
        result = conn.execute(stmt)
        conn.execute(text("COMMIT;"))
        return result.rowcount

inserted_rows = insert_roads_with_conflict_ignore(
    roads_df,
    'dwd_roads',
    target_engine
)
print(f"dwd_roads written: {inserted_rows} rows")

output_dir = os.path.expanduser('~/DataPlatform/process/DWD')
os.makedirs(output_dir, exist_ok=True)

roads_df['geom_wkt'] = roads_df['geom'].apply(lambda x: wkt_dumps(parse_geom(x)) if x else None)
roads_df.drop(columns=['geom'], inplace=True) 
roads_df.to_parquet(os.path.join(output_dir, 'roads.parquet'), index=False, engine='pyarrow')
print(f"Parquet files saved to {output_dir}")

print("dwd_roads sample:")
print(roads_df.head())
print("Map layer table (dwd_roads) created successfully in Aliyun RDS!")