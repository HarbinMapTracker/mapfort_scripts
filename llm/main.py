from fastapi import FastAPI, Depends, Query, HTTPException, status
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.utils import get_openapi
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime, timedelta
import time
import json
from database import get_db, TripInfo
from llm_service import LLMService
from config import get_llm_config, get_server_config, get_app_config

# 自定义JSONResponse类，确保中文字符显示正常
class CustomJSONResponse(JSONResponse):
    def render(self, content):
        return json.dumps(
            content,
            ensure_ascii=False,  # 确保中文字符不被转义
            allow_nan=False,
            indent=None,
            separators=(",", ":")
        ).encode("utf-8")

# 获取配置
llm_config = get_llm_config()
server_config = get_server_config()
app_config = get_app_config()

# 初始化LLM服务
llm_service = LLMService(
    api_key=llm_config["key"],
    base_url=llm_config["url"],
    model=llm_config["model"]
)



app = FastAPI(
    title="驾驶员历史出行分析API",
    description="用于分析驾驶员历史驾驶模式并提供休息建议的API服务",
    version="1.0.0",
    default_response_class=CustomJSONResponse  # 使用自定义的响应类
)

class DrivingPattern(BaseModel):
    """驾驶模式分析结果"""
    total_trips: int  # 总行程数
    total_driving_time_minutes: float  # 总驾驶时长(分钟)
    average_trip_duration_minutes: float  # 平均每次行程时长(分钟)
    night_driving_percentage: float  # 夜间驾驶比例(%)
    continuous_driving_incidents: int  # 连续驾驶事件数量
    longest_continuous_driving_minutes: float  # 最长连续驾驶时间(分钟)

class RestRecommendation(BaseModel):
    """休息建议"""
    needs_rest: bool  # 是否需要休息
    reason: str  # 需要休息的原因
    recommendation: str  # 具体休息建议
    fatigue_level: Optional[str] = None  # 疲劳等级（轻度/中度/重度/正常）
    who_standard_advice: Optional[str] = None  # WHO标准建议
    rest_methods: Optional[list] = None  # 具体休息方法列表（仅LLM模式）
    duration_advice: Optional[str] = None  # 休息时长建议（仅LLM模式）

class DriverAnalysis(BaseModel):
    """驾驶员分析结果"""
    devid: str  # 驾驶员设备ID
    driving_patterns: DrivingPattern  # 驾驶模式分析
    rest_recommendation: RestRecommendation  # 休息建议

def unix_to_datetime(timestamp: int) -> datetime:
    """将Unix时间戳转换为Python datetime对象"""
    return datetime.fromtimestamp(timestamp)

def is_night_driving(timestamp: int) -> bool:
    """检查给定的时间戳是否属于夜间驾驶（23:00-05:00）"""
    dt = unix_to_datetime(timestamp)
    hour = dt.hour
    return (hour >= 23) or (hour < 5)

def calculate_night_driving_minutes(trips):
    """计算夜间驾驶（23:00-05:00）的分钟数"""
    night_minutes = 0
    
    for trip in trips:
        start_dt = unix_to_datetime(trip.begin_time)
        end_dt = unix_to_datetime(trip.end_time)
        
        # 检查行程中每小时的驾驶情况，判断是否为夜间驾驶
        current = start_dt
        while current <= end_dt:
            if is_night_driving(int(current.timestamp())):
                # 计算夜间驾驶分钟数（最多60分钟或剩余行程时间）
                next_hour = (current + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                if next_hour > end_dt:
                    night_minutes += (end_dt - current).total_seconds() / 60
                else:
                    night_minutes += (next_hour - current).total_seconds() / 60
            current = current + timedelta(hours=1)
    
    return night_minutes

def find_continuous_driving(trips, rest_threshold_minutes=20):
    """
    查找连续驾驶且休息不足的事件（根据WHO标准，休息时长<20分钟视为连续驾驶）
    返回值: 连续驾驶事件数量和最长连续驾驶时间（分钟）
    """
    if not trips:
        return 0, 0
    
    # 按开始时间排序行程
    sorted_trips = sorted(trips, key=lambda x: x.begin_time)
    
    continuous_segments = []
    current_segment_start = sorted_trips[0].begin_time
    current_segment_end = sorted_trips[0].end_time
    
    for i in range(1, len(sorted_trips)):
        rest_period_minutes = (sorted_trips[i].begin_time - current_segment_end) / 60
        
        # 如果休息时间少于阈值，则延长当前连续驾驶片段
        if rest_period_minutes < rest_threshold_minutes:
            current_segment_end = sorted_trips[i].end_time
        else:
            # 存储当前片段并开始新的片段
            continuous_segments.append((current_segment_start, current_segment_end))
            current_segment_start = sorted_trips[i].begin_time
            current_segment_end = sorted_trips[i].end_time
    
    # 添加最后一个片段
    continuous_segments.append((current_segment_start, current_segment_end))
    
    # 计算最长连续驾驶时长（分钟）
    longest_duration = 0
    continuous_incidents = 0
    
    for start, end in continuous_segments:
        duration_minutes = (end - start) / 60
        
        # 如果连续驾驶超过3小时（180分钟），则计为一次事件
        if duration_minutes > 180:
            continuous_incidents += 1
            
        longest_duration = max(longest_duration, duration_minutes)
    
    return continuous_incidents, longest_duration

def assess_fatigue_level(continuous_driving_minutes: float, daily_driving_minutes: float, night_driving_minutes: float) -> tuple:
    """
    根据WHO疲劳驾驶指标评估疲劳等级
    
    Args:
        continuous_driving_minutes: 连续驾驶时长(分钟)
        daily_driving_minutes: 日累计驾驶时长(分钟)
        night_driving_minutes: 夜间驾驶时长(分钟)
    
    Returns:
        tuple: (疲劳等级, 休息建议)
    """
    # 转换为小时便于判断
    continuous_hours = continuous_driving_minutes / 60
    daily_hours = daily_driving_minutes / 60
    night_hours = night_driving_minutes / 60
    
    # 重度疲劳：连续驾驶>6小时 且 夜间驾驶>2小时
    if continuous_hours > 6 and night_hours > 2:
        return "重度", "停止驾驶，至少休息1小时"
    
    # 中度疲劳：连续驾驶>4小时 或 日累计驾驶>8小时
    if continuous_hours > 4 or daily_hours > 8:
        return "中度", "立即休息至少30分钟"
    
    # 轻度疲劳：连续驾驶3~4小时
    if 3 <= continuous_hours <= 4:
        return "轻度", "建议休息至少15分钟"
    
    # 无疲劳
    return "正常", "继续安全驾驶，注意适时休息"

def generate_rest_methods_by_level(fatigue_level: str) -> list:
    """
    根据疲劳等级生成休息方法建议
    """
    base_methods = [
        "下车进行5-10分钟步行，活动筋骨缓解久坐疲劳",
        "做颈部和肩部伸展运动，缓解驾驶姿势造成的肌肉紧张",
        "适量饮用温水，补充水分保持身体状态"
    ]
    
    if fatigue_level == "轻度":
        return base_methods + [
            "用冷水洗脸或湿毛巾敷眼部，提神醒脑恢复注意力",
            "在车内播放轻松音乐，进行3-5分钟深呼吸放松"
        ]
    elif fatigue_level == "中度":
        return base_methods + [
            "用冷水洗脸或湿毛巾敷眼部，提神醒脑恢复注意力",
            "在车内播放轻松音乐，进行5-10分钟深呼吸放松",
            "避免大量咖啡因摄入，可适量食用健康零食补充能量",
            "调节座椅到舒适位置，开窗通风保持空气流通"
        ]
    elif fatigue_level == "重度":
        return [
            "立即停车到安全地点，避免继续驾驶",
            "进行至少15-20分钟的步行活动，充分放松身体",
            "寻找舒适地点进行短时间休息或小憩",
            "联系家人或朋友，考虑换人驾驶或改用其他交通方式",
            "如条件允许，建议休息1-2小时后再继续驾驶",
            "避免依赖咖啡因强行提神，优先保证充分休息"
        ]
    else:  # 正常
        return [
            "保持良好的驾驶习惯，每2小时主动休息15分钟",
            "适时饮水，保持身体水分平衡", 
            "注意观察自身状态，如有疲劳感及时休息"
        ]

@app.get("/", 
    summary="API根路径",
    description="返回API服务的基本信息",
    tags=["系统"]
)
def read_root():
    """
    返回API服务的基本信息，用于检查API是否正常运行
    
    Returns:
        dict: 包含服务名称的简单消息
    """
    return {"message": "驾驶员历史出行分析API", "version": "1.0.0"}

@app.get("/driving-patterns/", 
    response_model=DrivingPattern,
    summary="获取驾驶员驾驶模式分析",
    description="""根据指定时间范围内的驾驶数据，分析驾驶员的驾驶模式和行为特征。    返回驾驶员的行为模式分析结果：
- 总行程数
- 总驾驶时长（分钟）
- 平均行程时长（分钟）
- 夜间驾驶比例（%）
- 连续驾驶事件数量
- 最长连续驾驶时间（分钟）""",
    tags=["驾驶分析"]
)
def get_driving_patterns(
    devid: str = Query(..., description="驾驶员设备ID"),
    simulated_time: Optional[int] = Query(None, description="模拟的当前时间（Unix时间戳）。如果未提供，将使用系统当前时间。"),
    days_back: int = Query(7, description="需要分析的历史天数"),
    db: Session = Depends(get_db)
):
    """
    分析驾驶员的历史驾驶模式，返回驾驶模式分析结果。
    
    - **devid**: 驾驶员设备ID
    - **simulated_time**: 可选参数，模拟的当前时间（Unix时间戳）
    - **days_back**: 需要分析的历史天数，默认为7天
    
    返回驾驶员的行为模式分析结果：
    - 总行程数
    - 总驾驶时长（分钟）
    - 平均行程时长（分钟）
    - 夜间驾驶比例（%）
    - 连续驾驶事件数量
    - 最长连续驾驶时间（分钟）
    """
    # 使用提供的模拟时间或当前时间
    current_time = simulated_time if simulated_time else int(time.time())
    current_dt = unix_to_datetime(current_time)
    
    # 计算分析的时间范围
    from_time = int((current_dt - timedelta(days=days_back)).timestamp())
    
    # 查询驾驶员在指定时间范围内的行程
    trips = db.query(TripInfo).filter(
        TripInfo.devid == devid,
        TripInfo.begin_time >= from_time,
        TripInfo.begin_time <= current_time
    ).all()
    
    if not trips:
        raise HTTPException(status_code=404, detail=f"在指定时间范围内未找到驾驶员 {devid} 的行程记录")
    
    # 计算驾驶模式
    total_trips = len(trips)
    total_driving_time_seconds = sum(trip.travel_time for trip in trips)
    total_driving_time_minutes = total_driving_time_seconds / 60
    avg_trip_duration_minutes = total_driving_time_minutes / total_trips if total_trips > 0 else 0
    
    night_driving_minutes = calculate_night_driving_minutes(trips)
    night_driving_percentage = (night_driving_minutes / total_driving_time_minutes * 100) if total_driving_time_minutes > 0 else 0
    
    continuous_incidents, longest_continuous = find_continuous_driving(trips)
    
    # 格式化响应数据
    driving_patterns = DrivingPattern(
        total_trips=total_trips,
        total_driving_time_minutes=round(total_driving_time_minutes, 1),
        average_trip_duration_minutes=round(avg_trip_duration_minutes, 1),
        night_driving_percentage=round(night_driving_percentage, 1),
        continuous_driving_incidents=continuous_incidents,
        longest_continuous_driving_minutes=round(longest_continuous, 1)
    )
    
    return driving_patterns

@app.get("/rest-recommendation/", 
    response_model=RestRecommendation,
    summary="获取休息建议",
    description="基于驾驶员历史驾驶数据，提供个性化的休息建议。可选择使用规则引擎或大模型生成。",
    tags=["驾驶建议"]
)
def get_rest_recommendation(
    devid: str = Query(..., description="驾驶员设备ID"),
    simulated_time: Optional[int] = Query(None, description="模拟的当前时间（Unix时间戳）。如果未提供，将使用系统当前时间。"),
    days_back: int = Query(7, description="需要分析的历史天数"),
    use_llm: bool = Query(False, description="是否使用LLM提供的休息建议，默认为否"),
    streaming: bool = Query(False, description="当use_llm=True时，是否使用流式响应，默认为否"),
    db: Session = Depends(get_db)
):
    """
    基于驾驶员的历史驾驶模式提供休息建议。
    可以选择使用规则引擎或LLM来生成建议。
    当use_llm=True时，可以选择流式或非流式响应。
    
    - **devid**: 驾驶员设备ID
    - **simulated_time**: 可选参数，模拟的当前时间（Unix时间戳）
    - **days_back**: 需要分析的历史天数，默认为7天
    - **use_llm**: 是否使用大模型提供建议，默认为否（使用规则引擎）
    - **streaming**: 当use_llm=True时，是否使用流式响应，默认为否
    
    返回休息建议包含：
    - 是否需要休息
    - 需要休息的原因
    - 具体的休息建议内容
    """
    # 使用提供的模拟时间或当前时间
    current_time = simulated_time if simulated_time else int(time.time())
    current_dt = unix_to_datetime(current_time)
    
    # 计算分析的时间范围
    from_time = int((current_dt - timedelta(days=days_back)).timestamp())
    
    # 查询驾驶员在指定时间范围内的行程
    trips = db.query(TripInfo).filter(
        TripInfo.devid == devid,
        TripInfo.begin_time >= from_time,
        TripInfo.begin_time <= current_time
    ).all()
    
    if not trips:
        raise HTTPException(status_code=404, detail=f"在指定时间范围内未找到驾驶员 {devid} 的行程记录")
    
    # 计算驾驶模式
    total_trips = len(trips)
    total_driving_time_seconds = sum(trip.travel_time for trip in trips)
    total_driving_time_minutes = total_driving_time_seconds / 60
    avg_trip_duration_minutes = total_driving_time_minutes / total_trips if total_trips > 0 else 0
    
    night_driving_minutes = calculate_night_driving_minutes(trips)
    night_driving_percentage = (night_driving_minutes / total_driving_time_minutes * 100) if total_driving_time_minutes > 0 else 0
    
    continuous_incidents, longest_continuous = find_continuous_driving(trips)
    
    # 最近24小时分析用于休息建议
    recent_time = int((current_dt - timedelta(days=1)).timestamp())
    recent_trips = [t for t in trips if t.begin_time >= recent_time]
    recent_driving_time_minutes = sum(trip.travel_time for trip in recent_trips) / 60
    
    # 创建驾驶模式数据
    driving_patterns = DrivingPattern(
        total_trips=total_trips,
        total_driving_time_minutes=round(total_driving_time_minutes, 1),
        average_trip_duration_minutes=round(avg_trip_duration_minutes, 1),
        night_driving_percentage=round(night_driving_percentage, 1),
        continuous_driving_incidents=continuous_incidents,
        longest_continuous_driving_minutes=round(longest_continuous, 1)
    )
    
    # 根据是否使用LLM区分处理
    if use_llm:
        # 准备LLM的输入数据
        driver_data = {
            "devid": devid,
            "driving_patterns": driving_patterns.dict()
        }
        
        # 如果请求流式响应
        if streaming:
            def generate():
                # 调用LLM服务获取流式建议
                for chunk in llm_service.get_rest_recommendation(driver_data, streaming=True):
                    if "error" in chunk:
                        yield f"data: {json.dumps({'error': chunk['error']}, ensure_ascii=False)}\n\n"
                        break
                        
                    if chunk.get("finished", False):
                        # 流结束，发送完整的建议
                        yield f"""data: {json.dumps({
                            'needs_rest': chunk.get('needs_rest', False),
                            'recommendation': chunk.get('complete_recommendation', ''),
                            'reason': '由LLM分析得出',
                            'finished': True
                        }, ensure_ascii=False)}\n\n"""
                    else:
                        # 发送部分响应内容
                        yield f"data: {json.dumps({'content': chunk.get('content', '')}, ensure_ascii=False)}\n\n"
            
            # 返回流式响应
            return StreamingResponse(
                generate(), 
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            )
        
        else:
            # 非流式LLM响应
            llm_response = llm_service.get_rest_recommendation(driver_data, streaming=False)
            
            if "error" in llm_response:
                raise HTTPException(status_code=500, detail=llm_response["error"])
              # 返回LLM生成的建议
            return RestRecommendation(
                needs_rest=llm_response.get("needs_rest", False),
                reason="由LLM基于WHO疲劳驾驶指标分析得出",
                recommendation=llm_response.get("recommendation", ""),
                fatigue_level=llm_response.get("fatigue_level", "未评估"),
                who_standard_advice=llm_response.get("duration_advice", ""),                rest_methods=llm_response.get("rest_methods", []),
                duration_advice=llm_response.get("duration_advice", "")
            )
    
    else:
        # 使用基于WHO疲劳驾驶指标的规则引擎
        
        # 计算最近24小时内的连续驾驶和夜间驾驶时长
        recent_continuous_driving = longest_continuous
        recent_night_driving = calculate_night_driving_minutes([t for t in trips if t.begin_time >= recent_time])
        
        # 评估疲劳等级
        fatigue_level, who_advice = assess_fatigue_level(
            continuous_driving_minutes=recent_continuous_driving,
            daily_driving_minutes=recent_driving_time_minutes,
            night_driving_minutes=recent_night_driving
        )
        
        # 根据疲劳等级设置休息建议
        if fatigue_level == "重度":
            needs_rest = True
            reason = f"根据WHO标准，您的疲劳等级为重度（连续驾驶{round(recent_continuous_driving/60, 1)}小时，夜间驾驶{round(recent_night_driving/60, 1)}小时）"
            recommendation = who_advice
        elif fatigue_level == "中度":
            needs_rest = True
            if recent_continuous_driving > 240:  # 连续驾驶>4小时
                reason = f"根据WHO标准，您的疲劳等级为中度（连续驾驶{round(recent_continuous_driving/60, 1)}小时）"
            else:
                reason = f"根据WHO标准，您的疲劳等级为中度（日累计驾驶{round(recent_driving_time_minutes/60, 1)}小时）"
            recommendation = who_advice
        elif fatigue_level == "轻度":
            needs_rest = True
            reason = f"根据WHO标准，您的疲劳等级为轻度（连续驾驶{round(recent_continuous_driving/60, 1)}小时）"
            recommendation = who_advice
        else:
            needs_rest = False
            reason = "根据WHO标准，当前疲劳等级正常"
            recommendation = who_advice
        
        # 返回包含WHO标准的休息建议
        return RestRecommendation(
            needs_rest=needs_rest,
            reason=reason,
            recommendation=recommendation,
            fatigue_level=fatigue_level,
            who_standard_advice=who_advice
        )

@app.get("/driver-analysis/", response_model=DriverAnalysis)
def get_driver_analysis(
    devid: str = Query(..., description="驾驶员设备ID"),
    simulated_time: Optional[int] = Query(None, description="模拟的当前时间（Unix时间戳）。如果未提供，将使用系统当前时间。"),
    days_back: int = Query(7, description="需要分析的历史天数"),
    use_llm: bool = Query(False, description="是否使用LLM提供休息建议，默认为否"),
    db: Session = Depends(get_db)
):
    """
    综合接口：同时分析驾驶员的历史驾驶模式并提供休息建议。
    此接口结合了 /driving-patterns/ 和 /rest-recommendation/ 的功能。
    """
    # 使用提供的模拟时间或当前时间
    current_time = simulated_time if simulated_time else int(time.time())
    current_dt = unix_to_datetime(current_time)
    
    # 计算分析的时间范围
    from_time = int((current_dt - timedelta(days=days_back)).timestamp())
    
    # 查询驾驶员在指定时间范围内的行程
    trips = db.query(TripInfo).filter(
        TripInfo.devid == devid,
        TripInfo.begin_time >= from_time,
        TripInfo.begin_time <= current_time
    ).all()
    
    if not trips:
        raise HTTPException(status_code=404, detail=f"在指定时间范围内未找到驾驶员 {devid} 的行程记录")
    
    # 计算驾驶模式
    total_trips = len(trips)
    total_driving_time_seconds = sum(trip.travel_time for trip in trips)
    total_driving_time_minutes = total_driving_time_seconds / 60
    avg_trip_duration_minutes = total_driving_time_minutes / total_trips if total_trips > 0 else 0
    
    night_driving_minutes = calculate_night_driving_minutes(trips)
    night_driving_percentage = (night_driving_minutes / total_driving_time_minutes * 100) if total_driving_time_minutes > 0 else 0
    
    continuous_incidents, longest_continuous = find_continuous_driving(trips)
    
    # 最近24小时分析用于休息建议
    recent_time = int((current_dt - timedelta(days=1)).timestamp())
    recent_trips = [t for t in trips if t.begin_time >= recent_time]
    recent_driving_time_minutes = sum(trip.travel_time for trip in recent_trips) / 60
    
    # 格式化驾驶模式数据
    driving_patterns = DrivingPattern(
        total_trips=total_trips,
        total_driving_time_minutes=round(total_driving_time_minutes, 1),
        average_trip_duration_minutes=round(avg_trip_duration_minutes, 1),
        night_driving_percentage=round(night_driving_percentage, 1),
        continuous_driving_incidents=continuous_incidents,
        longest_continuous_driving_minutes=round(longest_continuous, 1)
    )
    
    # 生成休息建议
    if use_llm:
        # 准备LLM的输入数据
        driver_data = {
            "devid": devid,
            "driving_patterns": driving_patterns.dict()
        }
        
        # 调用LLM生成建议
        llm_response = llm_service.get_rest_recommendation(driver_data, streaming=False)
        if "error" in llm_response:
            raise HTTPException(status_code=500, detail=llm_response["error"])
        
        # 设置LLM生成的建议
        rest_recommendation = RestRecommendation(
            needs_rest=llm_response.get("needs_rest", False),
            reason="由LLM基于WHO疲劳驾驶指标分析得出",
            recommendation=llm_response.get("recommendation", ""),
            fatigue_level=llm_response.get("fatigue_level", "未评估"),
            who_standard_advice=llm_response.get("duration_advice", ""),
            rest_methods=llm_response.get("rest_methods", []),
            duration_advice=llm_response.get("duration_advice", "")
        )
    else:
        # 使用基于WHO疲劳驾驶指标的规则引擎
        
        # 计算最近24小时内的连续驾驶和夜间驾驶时长
        recent_continuous_driving = longest_continuous
        recent_night_driving = calculate_night_driving_minutes([t for t in trips if t.begin_time >= recent_time])
        
        # 评估疲劳等级
        fatigue_level, who_advice = assess_fatigue_level(
            continuous_driving_minutes=recent_continuous_driving,
            daily_driving_minutes=recent_driving_time_minutes,
            night_driving_minutes=recent_night_driving
        )
        
        # 根据疲劳等级设置休息建议
        if fatigue_level == "重度":
            needs_rest = True
            reason = f"根据WHO标准，您的疲劳等级为重度（连续驾驶{round(recent_continuous_driving/60, 1)}小时，夜间驾驶{round(recent_night_driving/60, 1)}小时）"
            recommendation = who_advice
        elif fatigue_level == "中度":
            needs_rest = True
            if recent_continuous_driving > 240:  # 连续驾驶>4小时
                reason = f"根据WHO标准，您的疲劳等级为中度（连续驾驶{round(recent_continuous_driving/60, 1)}小时）"
            else:
                reason = f"根据WHO标准，您的疲劳等级为中度（日累计驾驶{round(recent_driving_time_minutes/60, 1)}小时）"
            recommendation = who_advice
        elif fatigue_level == "轻度":
            needs_rest = True
            reason = f"根据WHO标准，您的疲劳等级为轻度（连续驾驶{round(recent_continuous_driving/60, 1)}小时）"
            recommendation = who_advice
        else:
            needs_rest = False
            reason = "根据WHO标准，当前疲劳等级正常"
            recommendation = who_advice
                
        rest_recommendation = RestRecommendation(
            needs_rest=needs_rest,
            reason=reason,
            recommendation=recommendation,
            fatigue_level=fatigue_level,
            who_standard_advice=who_advice
        )
    
    # 返回完整的分析结果
    return DriverAnalysis(
        devid=devid,
        driving_patterns=driving_patterns,
        rest_recommendation=rest_recommendation
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)