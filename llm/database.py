from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# 数据库连接设置
DATABASE_URL = "postgresql://yjy:Yjy123456@rm-cn-9me49mca90004r8o.rwlb.rds.aliyuncs.com:5432/dataplatform"

# 创建SQLAlchemy引擎
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# 定义与dwd_trip_info表对应的TripInfo模型
class TripInfo(Base):
    __tablename__ = "dwd_trip_info"

    traj_id = Column(Integer, primary_key=True)  # 轨迹ID
    devid = Column(String)  # 设备ID
    travel_time = Column(Integer)  # 出行耗时（秒）
    begin_time = Column(Integer)  # 出行开始时间（Unix时间戳）
    end_time = Column(Integer)    # 出行结束时间（Unix时间戳）
    
    def to_dict(self):
        """将模型实例转换为字典，包含格式化的时间"""
        return {
            "traj_id": self.traj_id,
            "devid": self.devid,
            "travel_time": self.travel_time,
            "begin_time": self.begin_time,
            "end_time": self.end_time,
            "begin_time_formatted": datetime.fromtimestamp(self.begin_time).strftime('%Y-%m-%d %H:%M:%S'),
            "end_time_formatted": datetime.fromtimestamp(self.end_time).strftime('%Y-%m-%d %H:%M:%S'),
        }


# 用于获取数据库会话的依赖函数
def get_db():
    """依赖注入函数，提供数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()