import tushare as ts
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import time
import streamlit as st
from streamlit.runtime.secrets import Secrets

# 初始化Tushare API
# 注意：需要在环境变量或st.secrets中配置tushare token
def init_tushare_api():
    """
    初始化Tushare API
    """
    try:
        token = st.secrets["tushare"]["token"]
        ts.set_token(token)
        pro = ts.pro_api()
        return pro
    except Exception as e:
        print(f"初始化Tushare API失败: {e}")
        return None

def get_stock_list(pro):
    """
    获取所有A股股票列表
    """
    try:
        # 获取所有正常上市的股票
        stock_list = pro.stock_basic(
            exchange='', 
            list_status='L', 
            fields='ts_code,symbol,name,area,industry,market,list_date'
        )
        return stock_list
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return None

def get_stock_daily_data(pro, ts_code, start_date, end_date):
    """
    获取单只股票的日线行情数据
    """
    try:
        # 获取日线数据
        daily_data = pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        return daily_data
    except Exception as e:
        print(f"获取{ts_code}日线数据失败: {e}")
        return None

def init_database():
    """
    初始化数据库，创建表结构
    """
    try:
        # 从secrets.toml读取数据库连接信息
        conn = pymysql.connect(
            host=st.secrets["mysql"]["host"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"],
            charset='utf8mb4',
            autocommit=True,
            connect_timeout=600,
            read_timeout=600,
            write_timeout=600
        )
        
        cursor = conn.cursor()
        
        # 创建股票基本信息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_basic (
                ts_code VARCHAR(20) PRIMARY KEY,
                symbol VARCHAR(20),
                name VARCHAR(100),
                area VARCHAR(50),
                industry VARCHAR(100),
                market VARCHAR(20),
                list_date VARCHAR(20),
                update_time VARCHAR(50)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        
        # 创建股票日线行情表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_daily (
                id INT PRIMARY KEY AUTO_INCREMENT,
                ts_code VARCHAR(20),
                trade_date VARCHAR(20),
                open DECIMAL(10, 3),
                high DECIMAL(10, 3),
                low DECIMAL(10, 3),
                close DECIMAL(10, 3),
                pre_close DECIMAL(10, 3),
                `change` DECIMAL(10, 3),
                pct_chg DECIMAL(10, 3),
                vol DECIMAL(20, 3),
                amount DECIMAL(20, 3),
                UNIQUE KEY unique_ts_code_date (ts_code, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        
        # 创建索引以提高查询性能
        try:
            cursor.execute('''
                CREATE INDEX idx_ts_code_date ON stock_daily(ts_code, trade_date)
            ''')
        except:
            # 索引可能已存在，忽略错误
            pass
            
        try:
            cursor.execute('''
                CREATE INDEX idx_trade_date ON stock_daily(trade_date)
            ''')
        except:
            # 索引可能已存在，忽略错误
            pass
            
        try:
            cursor.execute('''
                CREATE INDEX idx_low_price ON stock_daily(low)
            ''')
        except:
            # 索引可能已存在，忽略错误
            pass
        
        cursor.close()
        conn.close()
        print("数据库初始化完成")
        return True
    except Exception as e:
        print(f"数据库初始化失败: {e}")
        return False

def save_stock_basic_to_db(pro, db_path=None):
    """
    保存股票基本信息到数据库
    """
    stock_list = get_stock_list(pro)
    if stock_list is None or stock_list.empty:
        print("未获取到股票列表数据")
        return
    
    try:
        # 从secrets.toml读取数据库连接信息
        conn = pymysql.connect(
            host=st.secrets["mysql"]["host"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"],
            charset='utf8mb4',
            autocommit=True
        )
        
        # 添加更新时间字段
        stock_list['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 清空现有数据
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stock_basic")
        cursor.close()
        
        # 批量插入数据
        data_tuples = [tuple(row) for row in stock_list.values]
        columns = ','.join(stock_list.columns)
        placeholders = ','.join(['%s'] * len(stock_list.columns))
        
        cursor = conn.cursor()
        insert_query = f"INSERT INTO stock_basic ({columns}) VALUES ({placeholders})"
        cursor.executemany(insert_query, data_tuples)
        cursor.close()
        
        conn.close()
        print(f"保存了 {len(stock_list)} 只股票的基本信息")
    except Exception as e:
        print(f"保存股票基本信息时出错: {e}")

def save_stock_daily_to_db(pro, days=120, db_path=None):
    """
    保存股票日线数据到数据库
    days: 获取最近多少天的数据
    """
    # 计算日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # 格式化日期
    end_date_str = end_date.strftime('%Y%m%d')
    start_date_str = start_date.strftime('%Y%m%d')
    
    # 获取股票列表
    stock_list = get_stock_list(pro)
    if stock_list is None or stock_list.empty:
        print("未获取到股票列表数据")
        return
    
    try:
        # 从secrets.toml读取数据库连接信息
        conn = pymysql.connect(
            host=st.secrets["mysql"]["host"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"],
            charset='utf8mb4',
            autocommit=True
        )
        
        total_stocks = len(stock_list)
        print(f"开始获取 {total_stocks} 只股票的历史数据，时间范围: {start_date_str} 至 {end_date_str}")
        
        # 逐个获取股票数据并保存
        for i, (_, stock) in enumerate(stock_list.iterrows()):
            ts_code = stock['ts_code']
            print(f"进度: {i+1}/{total_stocks} - 正在获取 {ts_code} 的数据...")
            
            # 获取该股票的日线数据
            daily_data = get_stock_daily_data(pro, ts_code, start_date_str, end_date_str)
            
            if daily_data is not None and not daily_data.empty:
                # 保存到数据库
                try:
                    # 批量插入数据
                    data_tuples = [tuple(row) for row in daily_data.values]
                    columns = ','.join([f"`{col}`" if col == 'change' else col for col in daily_data.columns])
                    placeholders = ','.join(['%s'] * len(daily_data.columns))
                    
                    # 增强重试机制
                    retry_count = 0
                    max_retries = 5
                    while retry_count < max_retries:
                        try:
                            conn = pymysql.connect(
                                host=st.secrets["mysql"]["host"],
                                user=st.secrets["mysql"]["user"],
                                password=st.secrets["mysql"]["password"],
                                database=st.secrets["mysql"]["database"],
                                charset='utf8mb4',
                                autocommit=True,
                                connect_timeout=60,
                                read_timeout=60,
                                write_timeout=60,
                            )
                            
                            cursor = conn.cursor()
                            insert_query = f"INSERT IGNORE INTO stock_daily ({columns}) VALUES ({placeholders})"
                            cursor.executemany(insert_query, data_tuples)
                            cursor.close()
                            conn.close()
                            break  # 成功执行则跳出重试循环
                        except (pymysql.OperationalError, pymysql.InterfaceError, pymysql.InternalError) as e:
                            retry_count += 1
                            print(f"数据库连接失败，正在重试 ({retry_count}/{max_retries}): {e}")
                            if retry_count >= max_retries:
                                print(f"达到最大重试次数，保存 {ts_code} 数据失败")
                                break
                            time.sleep(2 ** retry_count)  # 指数退避
                        except Exception as e:
                            # 非连接错误直接记录并跳出
                            print(f"保存 {ts_code} 数据时发生未预期错误: {e}")
                            break
                except Exception as e:
                    print(f"处理 {ts_code} 数据时出错: {e}")

            else:
                print(f"未获取到 {ts_code} 的数据")
            
            # 控制请求频率，避免被限制
            if (i + 1) % 100 == 0:
                print("休息1秒，避免请求过于频繁...")
                time.sleep(1)
            
            # 对于出现错误的股票，额外等待一段时间
            if daily_data is None or daily_data.empty:
                print(f"股票 {ts_code} 数据获取失败，短暂等待后继续...")
                time.sleep(0.5)
    except Exception as e:
        print(f"保存股票数据时出错: {e}")

def update_daily_data(pro, db_path=None):
    """
    每日更新最新数据
    """
    try:
        # 从secrets.toml读取数据库连接信息
        conn = pymysql.connect(
            host=st.secrets["mysql"]["host"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"],
            charset='utf8mb4',
            autocommit=True
        )
        
        # 获取昨天的日期
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        
        # 检查是否已经更新过昨天的数据
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM stock_daily WHERE trade_date = %s", (yesterday,))
        count = cursor.fetchone()[0]
        cursor.close()
        
        if count > 0:
            print(f"昨天({yesterday})的数据已存在，无需重复更新")
            conn.close()
            return
        
        # 获取股票列表
        stock_list = get_stock_list(pro)
        if stock_list is None or stock_list.empty:
            print("未获取到股票列表数据")
            conn.close()
            return
        
        print(f"开始更新 {yesterday} 的股票数据...")
        
        # 逐个获取股票最新数据并保存
        total_stocks = len(stock_list)
        for i, (_, stock) in enumerate(stock_list.iterrows()):
            ts_code = stock['ts_code']
            print(f"进度: {i+1}/{total_stocks} - 正在获取 {ts_code} 的数据...")
            
            # 获取该股票的最新数据
            try:
                daily_data = pro.daily(ts_code=ts_code, start_date=yesterday, end_date=yesterday)
                
                if daily_data is not None and not daily_data.empty:
                    # 保存到数据库，增加重试机制
                    data_tuples = [tuple(row) for row in daily_data.values]
                    columns = ','.join([f"`{col}`" if col == 'change' else col for col in daily_data.columns])
                    placeholders = ','.join(['%s'] * len(daily_data.columns))
                    
                    # 增强重试机制
                    retry_count = 0
                    max_retries = 5
                    while retry_count < max_retries:
                        try:
                            conn = pymysql.connect(
                                host=st.secrets["mysql"]["host"],
                                user=st.secrets["mysql"]["user"],
                                password=st.secrets["mysql"]["password"],
                                database=st.secrets["mysql"]["database"],
                                charset='utf8mb4',
                                autocommit=True,
                                connect_timeout=60,
                                read_timeout=60,
                                write_timeout=60,
                                max_allowed_packet=128*1024*1024  # 128MB
                            )
                            
                            cursor = conn.cursor()
                            insert_query = f"INSERT IGNORE INTO stock_daily ({columns}) VALUES ({placeholders})"
                            cursor.executemany(insert_query, data_tuples)
                            cursor.close()
                            conn.close()
                            break  # 成功执行则跳出重试循环
                        except (pymysql.OperationalError, pymysql.InterfaceError, pymysql.InternalError) as e:
                            retry_count += 1
                            print(f"数据库连接失败，正在重试 ({retry_count}/{max_retries}): {e}")
                            if retry_count >= max_retries:
                                print(f"达到最大重试次数，保存 {ts_code} 数据失败")
                                break
                            time.sleep(2 ** retry_count)  # 指数退避
                        except Exception as e:
                            #非连接错误直接记录并跳出
                            print(f"保存 {ts_code} 数据时发生未预期错误: {e}")
                            break
            except Exception as e:
                print(f"获取 {ts_code} 数据时出错: {e}")
            
            # 控制请求频率，避免被限制
            if (i + 1) % 100 == 0:
                print("休息1秒，避免请求过于频繁...")
                time.sleep(1)
        
        conn.close()
        print("每日数据更新完成")
    except Exception as e:
        print(f"更新每日数据时出错: {e}")

def query_stocks_with_double_tail_number(days=6, db_path=None):
    """
    查询最近N个交易日内最低价为双尾数（如1.33）的股票
    
    参数:
    days: 最近多少个交易日
    
    返回:
    符合条件的股票数据
    """
    try:
        # 从secrets.toml读取数据库连接信息
        conn = pymysql.connect(
            host=st.secrets["mysql"]["host"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"],
            charset='utf8mb4',
            autocommit=True
        )
        
        # 获取最近N个交易日的日期
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT trade_date 
            FROM stock_daily 
            ORDER BY trade_date DESC 
            LIMIT %s
        ''', (days,))
        
        recent_dates = cursor.fetchall()
        cursor.execute('''
                    SELECT DISTINCT trade_date 
                    FROM stock_daily 
                    ORDER BY trade_date DESC 
                    LIMIT %s
                ''', (3,))
        recent3_dates=cursor.fetchall()
        cursor.close()
        
        if not recent_dates:
            conn.close()
            print("未找到交易数据")
            return None
        
        # 构造日期条件
        date_list = [date[0] for date in recent_dates]
        date3_list=[date[0] for date in recent3_dates]
        # 查询在最近N天内最低价为双尾数的股票
        # 方法：先找出每个股票在最近N天内的最低价，然后筛选出最低价为双尾数的股票
        cursor = conn.cursor()
        format_strings = ','.join(['%s'] * len(date_list))
        print(format_strings)
        #最近3天
        format_str=','.join(['%s'] * len(date3_list))
        query = f'''
            SELECT s.name, s.ts_code, min_low.min_low_price as low
            FROM (
                SELECT ts_code, 
                       MIN(low) as min_low_price
                FROM stock_daily 
                WHERE trade_date IN ({format_strings})
                GROUP BY ts_code
            ) as min_low
            JOIN stock_basic s ON min_low.ts_code = s.ts_code
            join (select ts_code,min(low) low_3 from stock_daily  
                    WHERE trade_date IN ({format_str}) group by ts_code) min_3_day
            on min_low.ts_code=min_3_day.ts_code and min_low.min_low_price=min_3_day.low_3
            WHERE (
                (min_low.min_low_price * 100) - FLOOR(min_low.min_low_price * 100) = 0
                AND FLOOR(min_low.min_low_price * 100) %% 100 IN (
                    11, 22, 33, 44, 55, 66, 77, 88, 99
                )
            )            
        '''
        
        # 修复：将两个元组合并为一个元组作为查询参数
        params = tuple(date_list) + tuple(date3_list)
        cursor.execute(query, params)
        result = cursor.fetchall()
        cursor.close()
        
        # 转换为DataFrame
        if result:
            df = pd.DataFrame(result, columns=['name', 'ts_code','low'])
            conn.close()
            return df
        else:
            conn.close()
            return pd.DataFrame()
            
    except Exception as e:
        print(f"查询双尾数股票时出错: {e}")
        return None

# 主函数示例
def main():
    """
    主函数 - 初始化数据获取和存储
    """
    # 初始化Tushare API
    pro = init_tushare_api()
    if pro is None:
        print("Tushare API初始化失败")
        return
    
    # 初始化数据库
    # init_database()
    
    # 保存股票基本信息
    # print("正在获取并保存股票基本信息...")
    # save_stock_basic_to_db(pro)
    
    # 保存股票日线数据（最近120天）
    # print("正在获取并保存股票日线数据...")
    # save_stock_daily_to_db(pro, days=180)
    
    # print("数据初始化完成！")
    update_daily_data(pro)

if __name__ == "__main__":
    main()