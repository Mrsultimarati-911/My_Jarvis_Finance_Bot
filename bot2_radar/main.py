import os
import time
import datetime
import pandas as pd
import requests
import logging

BASE_DIR = "/app/data"
RAW_DATA_DIR = os.path.join(BASE_DIR, "bot2_data/raw_data")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Bot2] %(message)s')

def get_stock_name(code):
    """
    【修复版】利用新浪财经接口查名称 (抗封锁能力强)
    """
    try:
        # 判断市场前缀 (新浪需要 sz/sh 前缀)
        # 6开头是沪市(sh)，0或3开头是深市(sz)，4/8是北交所(bj)
        if code.startswith('6'):
            prefix = "sh"
        elif code.startswith('0') or code.startswith('3'):
            prefix = "sz"
        elif code.startswith('4') or code.startswith('8'):
            prefix = "bj"
        else:
            return "未知股票"

        # 新浪接口: http://hq.sinajs.cn/list=sz002131
        url = f"http://hq.sinajs.cn/list={prefix}{code}"
        
        # 新浪甚至不需要很复杂的 Headers
        headers = {"Referer": "https://finance.sina.com.cn/"}
        
        resp = requests.get(url, headers=headers, timeout=3)
        if resp.status_code == 200:
            # 返回格式: var hq_str_sz002131="利欧股份,1.980,..."
            text = resp.text
            if '="' in text:
                content = text.split('="')[1]
                if "," in content:
                    name = content.split(",")[0]
                    # 简单校验一下是不是空或者乱码
                    if len(name) > 0 and len(name) < 10:
                        return name
    except Exception as e:
        logging.warning(f"查名失败 {code}: {e}")
    
    return "未知股票"

def fetch_guba_rank():
    """
    【基于本地实测的抓取逻辑】
    """
    url = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/json"
    }
    
    payload = {
        "appId": "appId01",
        "globalId": "786e4c21-70dc-435a-93bb-38",
        "marketType": "",
        "pageNo": 1,
        "pageSize": 50
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code != 200: return []
        
        data = resp.json()
        rank_list = data.get('data', [])
        logging.info(f"抓取到 {len(rank_list)} 条原始数据")
        
        cleaned_list = []
        for item in rank_list:
            # --- 1. 代码清洗 (基于实测: SZ002131) ---
            raw_sc = str(item.get("sc", "")) # 拿到 "SZ002131"
            
            # 直接去掉前缀
            code = raw_sc.replace("SZ", "").replace("SH", "").strip()
            
            # 确保是纯数字且是6位
            if not code.isdigit() or len(code) != 6:
                continue
                
            # --- 2. 名字获取 ---
            # 既然已证实 API 不给名字，直接去查
            name = get_stock_name(code)
            
            # --- 3. 热度 ---
            # 之前的测试没看到 heat 字段，这里做一个防御，如果没有就给 0
            # 注意：之前的测试只打印了 sc, rk, rc, hisRc，说明可能没有 mainForce 了
            # 但为了兼容性，我们给个默认值
            heat = item.get("mainForce", 0)

            # --- 4. 存入 ---
            cleaned_list.append({
                "rank": item.get("rk"),
                "code": code, # 干净的 "002131"
                "name": name, # 干净的 "利欧股份"
                "heat": heat
            })
            
        return cleaned_list
            
    except Exception as e:
        logging.error(f"抓取异常: {e}")
        return []

def save_rank_data(date_str, rank_list):
    filename = f"{date_str}_bot2_rank.csv"
    filepath = os.path.join(RAW_DATA_DIR, filename)
    now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    rows = []
    for item in rank_list:
        item['datetime'] = now_time
        rows.append(item)
        
    df = pd.DataFrame(rows)
    if not df.empty:
        # 强制指定 code 为字符串类型，防止存入 csv 时变成数字
        df['code'] = df['code'].astype(str)
        
        # 重新排列列顺序
        df = df[["datetime", "rank", "code", "name", "heat"]]
        
        header = not os.path.exists(filepath)
        df.to_csv(filepath, mode='a', index=False, header=header, encoding='utf-8-sig', sep='|')
        logging.info(f"已保存 {len(rows)} 条数据")

def main():
    logging.info("Bot 2 (String Fix) 启动...")
    while True:
        try:
            ranks = fetch_guba_rank()
            if ranks:
                now_str = datetime.datetime.now().strftime("%Y_%m_%d")
                save_rank_data(now_str, ranks)
            else:
                logging.warning("抓取结果为空")
            time.sleep(300) 
        except Exception as e:
            logging.error(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
