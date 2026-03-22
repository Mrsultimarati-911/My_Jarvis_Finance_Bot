import os
import time
import datetime
import pandas as pd
import requests
import schedule
import logging
from lxml import etree
import random

# ================= 配置区 =================
BASE_DIR = "/app/data"
BOT2_RAW_DIR = os.path.join(BASE_DIR, "bot2_data/raw_data")
BOT3_DATA_DIR = os.path.join(BASE_DIR, "bot3_data")

# 调试开关：True 表示启动容器时立即跑一次
DEBUG_RUN_NOW = True 

# 设置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Bot3] %(message)s')
# =========================================

def get_current_date_str():
    return datetime.datetime.now().strftime("%Y_%m_%d")

def get_sina_code(code):
    """
    将 6 位代码转换为新浪需要的 sz/sh 格式
    """
    code = str(code).zfill(6) # 确保是6位
    if code.startswith('6'):
        return f"sh{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz{code}"
    elif code.startswith('4') or code.startswith('8'):
        return f"bj{code}"
    return code

def crawl_sina_guba(stock_code, stock_name):
    """
    【新浪版】爬取股吧标题
    """
    sina_code = get_sina_code(stock_code)
    url = f"https://guba.sina.com.cn/?s=bar&name={sina_code}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://finance.sina.com.cn/"
    }
    
    titles = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        
        if resp.status_code != 200:
            logging.warning(f"[{stock_name}] 请求失败: {resp.status_code}")
            return []
            
        # 新浪网页是 GBK 编码
        resp.encoding = 'gbk'
        
        html = etree.HTML(resp.text)
        
        # 尝试提取标题 (规则A)
        nodes = html.xpath('//div[contains(@class, "tit_01")]/a')
        
        # (规则B - 备用)
        if not nodes:
             nodes = html.xpath('//td/a[contains(@href, "tid")]')

        if not nodes:
            logging.warning(f"[{stock_name}] 未找到标题，可能是新股或冷门股")
            return []

        # 提取前 20 条
        for i, node in enumerate(nodes[:20]): 
            title_text = node.text
            if title_text:
                title_text = title_text.strip()
                if len(title_text) > 2:
                    titles.append({"rank": i+1, "title": title_text})
                
    except Exception as e:
        logging.error(f"[{stock_name}] 爬取异常: {e}")
        
    return titles

def run_miner_task():
    date_str = get_current_date_str()
    logging.info(f"🚀 开始挖掘任务 (新浪源): {date_str}")
    
    # 1. 读取 Bot 2 今天的数据
    bot2_file = os.path.join(BOT2_RAW_DIR, f"{date_str}_bot2_rank.csv")
    
    if not os.path.exists(bot2_file):
        logging.warning(f"Bot 2 数据未找到 ({bot2_file})，跳过。")
        return

    try:
        # 读取数据 (强制 code 为字符串)
        df = pd.read_csv(bot2_file, sep='|', dtype={'code': str})
        
        # 取前 5 名
        target_stocks = df.head(5)[['code', 'name']].to_dict('records')
        logging.info(f"🎯 今日目标: {target_stocks}")

        # 2. 准备目录
        today_dir = os.path.join(BOT3_DATA_DIR, date_str)
        if not os.path.exists(today_dir): os.makedirs(today_dir)

        # 3. 循环爬取
        for stock in target_stocks:
            code = stock['code']
            name = stock['name']
            
            logging.info(f"正在爬取: {name} ({code})...")
            titles = crawl_sina_guba(code, name)
            
            if titles:
                filename = f"{date_str}_{name}_{code}.csv"
                save_path = os.path.join(today_dir, filename)
                pd.DataFrame(titles).to_csv(save_path, index=False, encoding='utf-8-sig', sep='|')
                logging.info(f"✅ 已保存 {len(titles)} 条评论: {filename}")
            else:
                logging.warning(f"❌ {name} ({code}) 未抓取到数据")
            
            # === 这里就是之前报错的地方，现在的缩进是正确的 ===
            time.sleep(2) 

    except Exception as e:
        logging.error(f"挖掘任务出错: {e}")

def main():
    logging.info("Bot 3 (Sina Miner Final) 启动...")
    
    if DEBUG_RUN_NOW:
        run_miner_task()
        
    schedule.every().day.at("23:30").do(run_miner_task)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
