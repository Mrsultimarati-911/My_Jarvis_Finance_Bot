import re
import os
import time
import datetime
import pandas as pd
import requests
import logging
import json

# --- 配置 ---
BASE_DIR = "/app/data"
CONFIG_FILE = "/app/config/detection_target.txt"
RAW_DATA_DIR = os.path.join(BASE_DIR, "bot1_data/raw_data")
SUMMARY_DATA_DIR = os.path.join(BASE_DIR, "bot1_data/daily_summary_data")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Bot1] %(message)s')

def load_keywords():
    if not os.path.exists(CONFIG_FILE): return []
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def get_current_date_str():
    return datetime.datetime.now().strftime("%Y_%m_%d")

def fetch_real_news():
    """
    【实战修复版】抓取 7x24 快讯 (正则解析)
    """
    # 切换到 newsapi 域名，通常更稳定
    url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kuaixun.eastmoney.com/"
    }
    
    try:
        # 添加时间戳防止缓存
        timestamp = int(time.time() * 1000)
        full_url = f"{url}?r={timestamp}"
        
        resp = requests.get(full_url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            text = resp.text
            # 使用正则提取第一个 { ... } 结构，比 replace 更稳健
            match = re.search(r'(\{.*\})', text)
            if match:
                json_str = match.group(1)
                data = json.loads(json_str)
                return data.get('LivesList', [])
            else:
                logging.error("Bot1 未能在响应中匹配到 JSON 数据")
                
    except Exception as e:
        logging.error(f"Bot1 抓取失败: {e}")
        
    return []

def save_raw_data(date_str, news_item, target):
    filename = f"{date_str}_bot1_raw.csv"
    filepath = os.path.join(RAW_DATA_DIR, filename)
    
    # 东方财富的时间戳需要转换
    show_time = news_item.get('showtime', datetime.datetime.now().strftime("%H:%M:%S"))
    digest = news_item.get('digest', '')
    
    # 去除换行符，防止 CSV 格式错乱
    digest = digest.replace('\n', ' ').replace('\r', '')
    
    df = pd.DataFrame([[f"{date_str} {show_time}", digest, target]], columns=["datetime", "content", "target"])
    
    if not os.path.exists(filepath):
        df.to_csv(filepath, index=False, encoding='utf-8-sig', sep='|')
    else:
        df.to_csv(filepath, mode='a', index=False, header=False, encoding='utf-8-sig', sep='|')

def generate_daily_summary(date_str):
    # (此函数逻辑保持不变，为了节省篇幅，沿用之前的逻辑即可)
    # ... 请确保这里有之前的 generate_daily_summary 代码 ...
    # 如果你之前的代码被覆盖了，我可以再提供一次，但逻辑是一样的
    pass 
    # 注意：为了代码简洁，这里省略了 summary 函数，但在生产环境中必须保留！
    # 简单实现：
    raw_file = os.path.join(RAW_DATA_DIR, f"{date_str}_bot1_raw.csv")
    summary_file = os.path.join(SUMMARY_DATA_DIR, f"{date_str}_bot1_summary.csv")
    if not os.path.exists(raw_file): return
    try:
        df = pd.read_csv(raw_file, sep='|')
        if df.empty: return
        summary = df['target'].value_counts().reset_index()
        summary.columns = ['target', 'frequency']
        latest = df.sort_values('datetime').drop_duplicates('target', keep='last')[['target', 'content']]
        latest.columns = ['target', 'latest_content']
        res = pd.merge(summary, latest, on='target')
        res.to_csv(summary_file, index=False, encoding='utf-8-sig', sep='|')
        logging.info(f"总结生成: {summary_file}")
    except: pass

def main():
    logging.info("Bot 1 (Real News) 启动...")
    processed_ids = set() # 内存去重，防止重复保存同一条新闻
    current_date = get_current_date_str()
    
    while True:
        try:
            # 日切逻辑
            now_date = get_current_date_str()
            if now_date != current_date:
                generate_daily_summary(current_date)
                current_date = now_date
                processed_ids.clear() # 新的一天清空去重池
            
            keywords = load_keywords()
            news_list = fetch_real_news()
            logging.info(f"本次抓取到 {len(news_list)} 条快讯，正在匹配关键词...") # <--- 新增这行
            
            for news in news_list:
                news_id = news.get('id')
                content = news.get('digest', '')
                
                # 如果没处理过 且 内容非空
                if news_id not in processed_ids and content:
                    hit_target = None
                    for kw in keywords:
                        if kw in content:
                            hit_target = kw
                            break
                    
                    if hit_target:
                        logging.info(f"命中 [{hit_target}]: {content[:15]}...")
                        save_raw_data(now_date, news, hit_target)
                    
                    # 无论是否命中，都标记为已读，避免反复处理
                    processed_ids.add(news_id)
            
            # 限制集合大小，防止内存溢出
            if len(processed_ids) > 5000: processed_ids.clear()
            
            time.sleep(30) # 真实请求间隔调大一点，30秒一次足够
            
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
