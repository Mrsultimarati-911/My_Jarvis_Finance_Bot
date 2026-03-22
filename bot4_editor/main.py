import os
import datetime
import pandas as pd
import requests
import logging
import smtplib
import json
import time
import schedule
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

# ================= 配置与路径 =================
BASE_DIR = "/app/data"
BOT1_DIR = os.path.join(BASE_DIR, "bot1_data/raw_data")
BOT2_DIR = os.path.join(BASE_DIR, "bot2_data/raw_data")
BOT3_DIR = os.path.join(BASE_DIR, "bot3_data")

CONFIG_DIR = "/app/config"
EMAIL_CONFIG_FILE = os.path.join(CONFIG_DIR, "email_config.json")
API_KEY_FILE = os.path.join(CONFIG_DIR, "ai_api.txt")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Bot4] %(message)s')

# 全局变量
DEEPSEEK_API_KEY = ""
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
SMTP_SERVER = ""
SMTP_PORT = 465
EMAIL_USER = ""
EMAIL_PASS = ""
RECEIVER_EMAIL = ""

# ================= 核心功能函数 =================

def load_config():
    global DEEPSEEK_API_KEY, SMTP_SERVER, SMTP_PORT, EMAIL_USER, EMAIL_PASS, RECEIVER_EMAIL
    try:
        if os.path.exists(API_KEY_FILE):
            with open(API_KEY_FILE, 'r', encoding='utf-8') as f:
                DEEPSEEK_API_KEY = f.read().strip()
    except Exception as e:
        logging.error(f"加载 API Key 失败: {e}")

    try:
        if os.path.exists(EMAIL_CONFIG_FILE):
            with open(EMAIL_CONFIG_FILE, 'r', encoding='utf-8') as f:
                conf = json.load(f)
                SMTP_SERVER = conf.get("smtp_server")
                SMTP_PORT = int(conf.get("smtp_port", 465))
                EMAIL_USER = conf.get("sender_email")
                EMAIL_PASS = conf.get("sender_password")
                RECEIVER_EMAIL = conf.get("receiver_email")
    except Exception as e:
        logging.error(f"加载邮件配置失败: {e}")

def get_current_date():
    return datetime.datetime.now().strftime("%Y_%m_%d")

def get_yesterday_date():
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y_%m_%d")

def get_bot1_data():
    """获取快讯 (已修复文件名)"""
    data_points = []
    # 扩大搜索范围，防止因为时差漏掉数据
    for date_str in [get_yesterday_date(), get_current_date()]:
        # ⚠️ 关键修正：文件名必须是 bot1_raw.csv
        file_path = os.path.join(BOT1_DIR, f"{date_str}_bot1_raw.csv")
        if os.path.exists(file_path):
            try:
                # 读取时处理可能存在的格式问题
                df = pd.read_csv(file_path, sep='|', names=['datetime', 'keyword', 'content'], on_bad_lines='skip')
                # 只取最近的 20 条给 AI 筛选
                data_points.extend(df.tail(20).to_dict('records'))
            except Exception as e: 
                logging.error(f"读取 Bot1 文件 {file_path} 失败: {e}")
                pass
    return data_points[-20:] if data_points else []

def get_bot2_data():
    """获取榜单"""
    date_str = get_current_date()
    file_path = os.path.join(BOT2_DIR, f"{date_str}_bot2_rank.csv")
    if not os.path.exists(file_path):
        date_str = get_yesterday_date()
        file_path = os.path.join(BOT2_DIR, f"{date_str}_bot2_rank.csv")
    
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, sep='|', dtype={'code': str})
            return df.head(10)[['code', 'name', 'heat']].to_dict('records')
        except: return []
    return []

def get_bot3_raw_text():
    """获取舆情"""
    target_date = get_yesterday_date()
    target_dir = os.path.join(BOT3_DIR, target_date)
    
    if not os.path.exists(target_dir) or not os.listdir(target_dir):
        return None

    combined_text = ""
    csv_files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
    for file in csv_files:
        try:
            parts = file.replace(".csv", "").split("_")
            stock_name = parts[-2] if len(parts) >= 3 else file
            df = pd.read_csv(os.path.join(target_dir, file), sep='|')
            titles = df['title'].head(10).tolist()
            clean_titles = [t.replace('\n', ' ') for t in titles]
            combined_text += f"\n=== {stock_name} ===\n" + "\n".join(clean_titles) + "\n"
        except: pass
    return combined_text

def analyze_with_deepseek(bot1_data, bot2_data, bot3_text):
    """
    【升级版 Prompt】要求 AI 必须返回 news_summary
    """
    if not DEEPSEEK_API_KEY:
        return {}

    system_prompt = "你是一位资深的A股量化舆情分析师。请根据提供的数据，输出结构化的日报内容。"
    user_prompt = f"""
    【输入数据】
    1. 全球宏观快讯(原始数据)：{str(bot1_data)}
    2. A股热点榜：{str(bot2_data)}
    3. 散户舆情(重点分析对象)：
    {bot3_text if bot3_text else "暂无详细评论数据"}

    【输出要求】
    必须返回标准的 **JSON 格式**，包含以下三个字段：
    
    1. "news_summary": 一个列表。请从“全球宏观快讯”中总结和提取五则焦点事件，每个焦点聚焦一个关键词，进行润色总结（50-100字）。
       格式：[ "焦点1内容...", "焦点2内容..." ]
    
    2. "market_summary": 一段话总结市场整体多空情绪 (100字以内)。
    
    3. "stocks": 一个列表，对每一只在“散户舆情”中出现的股票进行分析。
       每只股票包含：
       - "name": 股票名称
       - "code": 股票代码
       - "sentiment": "bullish"(看多) / "bearish"(看空) / "neutral"(观望)
       - "tag": "乐观" / "悲观" / "分歧"
       - "reason": 分析理由 (50字以内)

    【JSON 示例】
    {{
      "news_summary": [
          "综合来看，黄金目前的暴跌受美联储下一任主席提名影响明显……。",
          "DOGE币经历暴跌后首次止跌，……。"
      ],
      "market_summary": "市场整体情绪...",
      "stocks": [...]
    }}
    """
    
    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        "temperature": 0.3, "stream": False
    }
    
    try:
        resp = requests.post(DEEPSEEK_URL, json=payload, headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}"})
        result = resp.json()['choices'][0]['message']['content']
        clean_json = result.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_json)
    except Exception as e:
        logging.error(f"AI 分析失败: {e}")
        return {"news_summary": [], "market_summary": "AI 接口异常", "stocks": []}

def generate_html(analysis_data, bot2_data):
    """
    【升级版 HTML】直接读取 AI 生成的新闻摘要
    """
    css = """
    <style>
        body { font-family: 'Helvetica Neue', Arial, sans-serif; background-color: #f4f4f4; color: #333; margin:0; padding:0;}
        .container { max-width: 600px; margin: 20px auto; background: #fff; padding: 0; border-radius: 8px; overflow: hidden; }
        .header { background: #d32f2f; color: #fff; padding: 25px; text-align: center; }
        .header h1 { margin:0; font-size: 22px; }
        .section { padding: 20px; border-bottom: 1px solid #eee; }
        .section-title { font-size: 16px; font-weight: bold; border-left: 4px solid #d32f2f; padding-left: 10px; margin-bottom: 15px; color: #333; }
        .news-item { margin-bottom: 10px; font-size: 14px; line-height: 1.5; color: #555; background: #f9f9f9; padding: 10px; border-radius: 4px; }
        .rank-item { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; }
        .sentiment-card { display: flex; align-items: center; background: #f9f9f9; border-radius: 6px; padding: 12px; margin-bottom: 10px; border-left: 4px solid #ccc; }
        .bullish { border-left-color: #e53935; background: #fff5f5; }
        .bearish { border-left-color: #43a047; background: #f1f8e9; }
        .neutral { border-left-color: #757575; background: #f5f5f5; }
        .stock-name { font-weight: bold; font-size: 15px; }
        .reason { font-size: 12px; color: #666; margin-top: 4px; }
        .tag { font-size: 12px; padding: 2px 6px; border-radius: 4px; color: #fff; margin-left: auto; }
        .bg-red { background: #e53935; } .bg-green { background: #43a047; } .bg-grey { background: #757575; }
        .footer { text-align: center; font-size: 12px; color: #999; padding: 20px; background: #fafafa; }
    </style>
    """

    # 1. 宏观快讯 (从 AI 结果中读取)
    news_list = analysis_data.get('news_summary', [])
    news_html = ""
    if news_list:
        for item in news_list:
            news_html += f"<div class='news-item'>• {item}</div>"
    else:
        news_html = "<div style='color:#999; font-style:italic;'>AI 未筛选出重大快讯，或原始数据为空。</div>"

    # 2. 榜单 HTML
    rank_html = ""
    for i, item in enumerate(bot2_data):
        rank_html += f"<div class='rank-item'><span style='font-weight:bold; color:#d32f2f; width:20px;'>{i+1}</span> <span>{item['name']}</span> <span style='color:#999;'>{item['code']}</span></div>"

    # 3. 舆情 HTML
    sentiment_html = f"<div style='background:#fff8e1; padding:10px; font-size:13px; color:#795548; margin-bottom:15px; border-radius:4px;'>⚖️ <b>总评：</b>{analysis_data.get('market_summary', '暂无')}</div>"
    
    for stock in analysis_data.get('stocks', []):
        sent = stock.get('sentiment', 'neutral')
        s_cls = "bullish" if sent == 'bullish' else "bearish" if sent == 'bearish' else "neutral"
        tag_bg = "bg-red" if sent == 'bullish' else "bg-green" if sent == 'bearish' else "bg-grey"
        
        sentiment_html += f"""
        <div class="sentiment-card {s_cls}">
            <div style="flex:1;">
                <div class="stock-name">{stock['name']} <span style="color:#999; font-size:12px;">{stock['code']}</span></div>
                <div class="reason">{stock['reason']}</div>
            </div>
            <div class="tag {tag_bg}">{stock['tag']}</div>
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html><head>{css}</head><body>
    <div class="container">
        <div class="header"><h1>📊 A股早报 ({get_current_date()})</h1></div>
        <div class="section"><div class="section-title">板块一：全球宏观精选 (AI)</div>{news_html}</div>
        <div class="section"><div class="section-title">板块二：资金热榜</div>{rank_html}</div>
        <div class="section"><div class="section-title">板块三：舆情多空研判</div>{sentiment_html}</div>
        <div class="footer">Powered by DeepSeek & Python Bot</div>
    </div></body></html>
    """

def send_email(html_content):
    if not SMTP_SERVER or not EMAIL_USER: return
    msg = MIMEMultipart()
    msg['From'] = Header(f"金融情报 <{EMAIL_USER}>", 'utf-8')
    msg['To'] = Header("主理人", 'utf-8')
    msg['Subject'] = Header(f"【早报】{get_current_date()} 市场情报", 'utf-8')
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))
    try:
        s = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) if SMTP_PORT == 465 else smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        s.login(EMAIL_USER, EMAIL_PASS)
        s.sendmail(EMAIL_USER, RECEIVER_EMAIL, msg.as_string())
        s.quit()
        logging.info("✅ 邮件发送成功")
    except Exception as e: logging.error(f"❌ 发送失败: {e}")

def run_daily_task():
    logging.info("⏰ 开始执行任务...")
    load_config()
    
    # 收集数据
    bot1 = get_bot1_data()
    bot2 = get_bot2_data()
    bot3_text = get_bot3_raw_text()
    
    # 如果完全没数据，就不分析了
    if not bot1 and not bot3_text:
        logging.warning("数据源为空，跳过 AI 分析")
        return

    # AI 分析
    logging.info(f"正在分析... (Bot1快讯数: {len(bot1)})")
    analysis = analyze_with_deepseek(bot1, bot2, bot3_text)
    
    # 生成报告
    html = generate_html(analysis, bot2)
    send_email(html)
    logging.info("✅ 任务完成")

def main():
    run_daily_task()
    logging.info("Bot 4 (Final Fix) 启动... 06:00 待机中")
    schedule.every().day.at("06:00").do(run_daily_task)
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
