"""
配置模块，用于管理应用程序的配置参数
支持从环境变量或配置文件加载配置
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv
import json

# 尝试加载.env文件中的环境变量（如果存在）
load_dotenv()

# LLM服务默认配置
DEFAULT_LLM_CONFIG = {
    "key": "your_default_api_key_here",
    "url": "https://api.example.com/v1",
    "model": "default-model"
}

# 服务器默认配置
DEFAULT_SERVER_CONFIG = {
    "host": "0.0.0.0",
    "port": 8000
}

# 应用程序参数默认配置
DEFAULT_APP_CONFIG = {
    "night_driving_start_hour": 23,  # 夜间驾驶开始时间（小时）
    "night_driving_end_hour": 5,     # 夜间驾驶结束时间（小时）
    "continuous_driving_threshold": 240,  # 连续驾驶阈值（分钟）
    "rest_threshold_minutes": 15,    # 休息阈值（分钟）
    "daily_driving_limit": 600       # 每日驾驶限制（分钟）
}

def _load_config_file(file_path: str) -> Dict[str, Any]:
    """从文件加载配置"""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"加载配置文件 {file_path} 失败: {str(e)}")
    return {}

# 尝试从配置文件加载配置
CONFIG_FILE_PATH = os.getenv("CONFIG_FILE_PATH", "config.json")
file_config = _load_config_file(CONFIG_FILE_PATH)

def get_llm_config() -> Dict[str, str]:
    """获取LLM服务配置"""
    # 优先从环境变量获取
    config = {
        "key": os.getenv("LLM_API_KEY"),
        "url": os.getenv("LLM_API_URL"),
        "model": os.getenv("LLM_MODEL")
    }
    
    # 如果环境变量没有设置，从配置文件获取
    file_llm_config = file_config.get("llm", {})
    
    # 合并配置，优先级：环境变量 > 配置文件 > 默认配置
    result = {**DEFAULT_LLM_CONFIG, **file_llm_config}
    result.update({k: v for k, v in config.items() if v is not None})
    print(f"LLM配置: {result}")
    return result

def get_server_config() -> Dict[str, Any]:
    """获取服务器配置"""
    # 从环境变量获取
    try:
        port = int(os.getenv("SERVER_PORT", "0"))
    except ValueError:
        port = 0
    
    config = {
        "host": os.getenv("SERVER_HOST"),
        "port": port if port > 0 else None
    }
    
    # 从配置文件获取
    file_server_config = file_config.get("server", {})
    
    # 合并配置
    result = {**DEFAULT_SERVER_CONFIG, **file_server_config}
    result.update({k: v for k, v in config.items() if v is not None})
    
    return result

def get_app_config() -> Dict[str, Any]:
    """获取应用程序参数配置"""
    # 从配置文件获取
    file_app_config = file_config.get("app", {})
    
    # 合并配置
    return {**DEFAULT_APP_CONFIG, **file_app_config}
