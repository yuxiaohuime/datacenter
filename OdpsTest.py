import pandas as pd
import pandas as pd
from odps import ODPS

o = ODPS('LTAI5tEHcsAw6c9P3TqwtiMd', 'AzM3KGo4oMcjqWYt3llArBUw7ZC90P', 'yswy_ads',
         endpoint='http://service.cn-hangzhou.maxcompute.aliyun.com/api')



def fetch_demo_data():
    """从ODPS获取直播数据"""
    reader = o.execute_sql("""
        SELECT  用户昵称,用户手机号,积分,CONCAT_WS(',',添加的企微成员) 添加的企微成员,企微是否加对了团长,日期,周,
                                        round(sum(看播时长),0) as 看播时长,round(sum(领取积分),0) as 领取积分,round(sum(金额),1) as 下单金额,累计看播时长,累计领取积分,累计金额
                                FROM    yswy_ads.ads_lzt_customer_analysis_30_df
                                WHERE   ds = MAX_PT('yswy_ads.ads_lzt_customer_analysis_30_df')
                                        
                                        and 累计看播时长<>0 and `门店`='宁波一店（大卿桥店）' GROUP BY 用户昵称,用户手机号,积分,添加的企微成员,企微是否加对了团长,日期,周,累计看播时长,累计领取积分,累计金额

    """).open_reader(tunnel=True, limit=False)

    # 转换为DataFrame
    data = []
    for record in reader:
        data.append(record.values)

    columns = [col.name for col in reader.schema.columns]
    return pd.DataFrame(data, columns=columns)



def df_pivot(df, multi_columns, agg_column, columns_to_pivot, agg_func):
    # 提取索引列名（从multi_columns元组中提取第二项）
    index_columns = [col[len(col) - 1] for col in multi_columns]

    # 确保agg_column是列表格式
    agg_columns = agg_column if isinstance(agg_column, list) else [agg_column]

    # 创建透视表
    df_pivot = df.pivot_table(
        index=index_columns,
        columns=agg_columns,
        values=columns_to_pivot,
        aggfunc=agg_func
    ).reset_index()

    # 如果是多级列索引，重新组织列名结构
    if isinstance(df_pivot.columns, pd.MultiIndex):
        # 获取现有的列结构
        original_columns = df_pivot.columns.tolist()
        # 构建新的多级列索引
        new_columns = []

        # 添加索引列的多级标题
        for col in multi_columns:
            new_columns.append((col[0], col[1], col[2]))  # 三级标题结构

        # 处理数据列的多级标题
        for col in original_columns[len(index_columns):]:

            if isinstance(col, tuple):
                # 数据列标题结构为 (周, 日期, 统计值)
                metric, date, week = col
                new_columns.append((week, date, metric))
            else:
                # 其他情况，保持原样
                new_columns.append((col,))

        # 设置新的多级列索引
        df_pivot.columns = pd.MultiIndex.from_tuples(new_columns)

    return df_pivot


df = fetch_demo_data()

# 选择需要进行行转列的列
columns_to_pivot = ['看播时长', '领取积分', '下单金额']
multi_columns = [
    ('', '', '用户昵称'),
    ('', '', '用户手机号'),
    ('', '', '添加的企微成员'),
    ('', '', '企微是否加对了团长'),
    ('', '', '积分'),
    ('', '', '累计看播时长'),
    ('', '', '累计领取积分'),
    ('', '', '累计金额')
]

df_export = df_pivot(df, multi_columns, [ '日期','周'], columns_to_pivot, 'sum')
cols = df_export.columns.tolist()
cols.sort(key=cmp_to_key(compare))
new_df=df_export[cols]
res=new_df.sort_values(('', '', '累计金额'), ascending=False)
# # 打印结果
# print(df_export)
#
# 导出到CSV文件
res.to_csv('D:\\Downloads\\output.csv', index=False, encoding='gbk',errors='ignore')




