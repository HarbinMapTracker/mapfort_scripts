from openai import OpenAI
import json5
from typing import Optional, Dict, Any, Generator


class LLMService:
    """LLM服务类，封装对OpenAI API的调用"""

    def __init__(self, api_key: str, base_url: str, model: str):
        """
        初始化LLM服务
        
        Args:
            api_key: API密钥
            base_url: API基础URL
            model: 使用的模型名称
        """
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        print(api_key, base_url, model)
        self.client = OpenAI(
            api_key=api_key,
            base_url=base_url,
        )
        
    def get_rest_recommendation(self, driver_data: Dict[str, Any], streaming: bool = False) -> Any:
        """
        获取休息建议
        
        Args:
            driver_data: 驾驶员数据
            streaming: 是否使用流式响应
            
        Returns:
            如果streaming=False，返回完整响应
            如果streaming=True，返回流式响应的生成器        """
        # 构建提示消息
        prompt = self._build_prompt(driver_data)
        
        if streaming:
            # 流式响应
            return self._get_streaming_recommendation(prompt)
        else:
            # 非流式响应
            return self._get_recommendation(prompt)
    
    def _build_prompt(self, driver_data: Dict[str, Any]) -> str:
        """构建提示消息"""
        patterns = driver_data.get("driving_patterns", {})
        recent_data = driver_data.get("recent_driving", {})
        
        # 构建包含休息方法的中文提示
        prompt = f"""
作为驾驶安全顾问，请根据以下数据提供详细的休息建议和具体的休息方法，并以JSON5格式返回：

【七天内驾驶数据】
总行程: {patterns.get('total_trips')} 次
总驾驶: {patterns.get('total_driving_time_minutes', 0)} 分钟
平均行程: {patterns.get('average_trip_duration_minutes', 0)} 分钟
夜间驾驶: {patterns.get('night_driving_percentage', 0)}%
连续驾驶事件: {patterns.get('continuous_driving_incidents', 0)} 次
最长连续驾驶: {patterns.get('longest_continuous_driving_minutes', 0)} 分钟

【今日驾驶数据】
今日行程: {recent_data.get('today_trips', 0)} 次
今日驾驶: {recent_data.get('today_driving_minutes', 0)} 分钟
当前是否夜间: {recent_data.get('is_night_now', False)}

请根据世界卫生组织疲劳驾驶指标，提供详细的休息建议和具体的休息方法。

世界卫生组织疲劳驾驶指标：
- 轻度疲劳：连续驾驶3-4小时，建议休息至少15分钟
- 中度疲劳：连续驾驶>4小时或日累计驾驶>8小时，立即休息至少30分钟
- 重度疲劳：连续驾驶>6小时且夜间驾驶>2小时，停止驾驶，至少休息1小时

请以JSON5格式返回，必须包含以下字段:
{{
  needs_rest: true/false,  // 布尔值，表示是否需要休息
  fatigue_level: "正常/轻度/中度/重度",  // 疲劳等级
  recommendation: "休息建议文本",  // 简短的总体建议（50字以内）
  rest_methods: [  // 具体的休息方法列表
    "方法1：具体描述",
    "方法2：具体描述", 
    "方法3：具体描述"
  ],
  duration_advice: "建议的休息时长"  // 基于WHO标准的具体时长建议
}}

休息方法应包括但不限于：
1. 身体放松类：深呼吸、伸展运动、眼部按摩等
2. 环境调节类：开窗通风、调节座椅、播放轻音乐等
3. 饮食补充类：适量饮水、健康零食、避免过量咖啡因等
4. 心理调节类：冥想、听音乐、与家人通话等
5. 活动类：下车走动、简单运动、换人驾驶等

请使用友好专业的语气，给出实用有效的建议。

例如: {{ 
  needs_rest: true, 
  fatigue_level: "中度",
  recommendation: "您已连续驾驶4.5小时，根据WHO标准达到中度疲劳，建议立即休息30分钟",
  rest_methods: [
    "下车进行5-10分钟步行，活动筋骨缓解久坐疲劳",
    "做颈部和肩部伸展运动，缓解驾驶姿势造成的肌肉紧张", 
    "用冷水洗脸或湿毛巾敷眼部，提神醒脑恢复注意力",
    "适量饮用温水，避免大量咖啡因摄入",
    "在车内播放轻松音乐，进行3-5分钟深呼吸放松"
  ],
  duration_advice: "根据WHO标准，中度疲劳需休息至少30分钟"
}}
"""
        return prompt
    
    def _get_recommendation(self, prompt: str) -> Dict[str, Any]:
        """获取非流式休息建议"""
        try:            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是驾驶安全顾问，根据WHO疲劳驾驶指标提供详细的休息建议和具体方法。请以JSON5格式返回，包含needs_rest、fatigue_level、recommendation、rest_methods和duration_advice字段。"},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            recommendation_text = response.choices[0].message.content
            
            # 解析JSON响应
            recommendation_data = json5.loads(recommendation_text)
            
            return {
                "needs_rest": recommendation_data.get("needs_rest", False),
                "fatigue_level": recommendation_data.get("fatigue_level", "正常"),
                "recommendation": recommendation_data.get("recommendation", ""),
                "rest_methods": recommendation_data.get("rest_methods", []),
                "duration_advice": recommendation_data.get("duration_advice", ""),
                "is_streaming": False
            }
        except Exception as e:
            return {
                "error": f"获取建议时出错: {str(e)}",
                "is_streaming": False
            }
    
    def _get_streaming_recommendation(self, prompt: str) -> Generator[Dict[str, Any], None, None]:
        """获取流式休息建议"""
        try:            
            response_stream = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是驾驶安全顾问，提供简短的休息建议，适合TTS播报。回复应简洁明了，控制在50字以内。"},
                    {"role": "user", "content": prompt}
                ],
                stream=True
            )
            
            full_response = ""
            
            for chunk in response_stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    content = chunk.choices[0].delta.content
                    full_response += content
                    yield {
                        "content": content,
                        "is_streaming": True
                    }
            
            # 在流结束时，返回完整的建议和需要休息的判断
            yield {
                "needs_rest": self._determine_needs_rest(full_response),
                "complete_recommendation": full_response,
                "is_streaming": True,
                "finished": True
            }
            
        except Exception as e:
            yield {
                "error": f"获取流式建议时出错: {str(e)}",
                "is_streaming": True,
                "finished": True
            }
    
    def _determine_needs_rest(self, recommendation: str) -> bool:
        """
        根据LLM的建议判断是否需要休息
        简单实现: 检查文本中是否包含需要休息的关键词
        """
        rest_keywords = [
            "需要休息", "应该休息", "建议休息", "休息是必要的", 
            "立即休息", "停车休息", "休息一下"
        ]
        
        for keyword in rest_keywords:
            if keyword in recommendation:
                return True
                
        return False
