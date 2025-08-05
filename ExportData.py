import pandas as pd

from functools import cmp_to_key
# 定义比较函数
def compare_lzt_date_by_shop(x, y):
    x_v=x[1]
    y_v=y[1]
    # 自定义比较逻辑
    if x[1]=='':
        x_v='0'
    if y[1]=='':
        y_v='0'
    if x_v < y_v:
        return -1
    elif x_v > y_v:
        return 1
    else:
        return 0

def df_pivot(df, multi_columns, agg_column, columns_to_pivot, agg_func):
    # 提取索引列名（从multi_columns元组中提取第二项）
    new_df=df
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
        cols = df_pivot.columns.tolist()
        #字段排序
        cols.sort(key=cmp_to_key(compare_lzt_date_by_shop))
        new_df = df_pivot[cols]
        #累计金额 倒序

    return new_df.sort_values(('', '', '累计金额'), ascending=False)
def export_lzt_date_by_shop(st,o):
    try:
        # 获取对应的SQL查询
        if 'query_sql' in st.session_state:
            query_sql = st.session_state['query_sql']
            # 执行查询并获取reader
            reader = o.execute_sql(query_sql).open_reader(tunnel=True, limit=False)

            # 转换为DataFrame
            data = []
            for record in reader:
                data.append(record.values)

            columns = [col.name for col in reader.schema.columns]
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
            columns_to_pivot = ['看播时长', '领取积分', '下单金额']
            df_export = df_pivot(pd.DataFrame(data, columns=columns), multi_columns, ['日期', '周'], columns_to_pivot,
                                 'sum')
            file = df_export.to_csv(index=False, encoding='gbk', errors='ignore')
            df_export.to_csv('D:\\Downloads\\output.csv', index=False, encoding='gbk', errors='ignore')
            encoding_name = 'GBK'
            # 简化版本
            import io

            # 创建字节缓冲区
            csv_buffer = io.BytesIO()

            # 将DataFrame写入缓冲区（返回None，数据在缓冲区中）
            df_export.to_csv(csv_buffer, index=False, encoding='gbk', errors='ignore')

            # 重置指针到开始位置
            csv_buffer.seek(0)

            # 获取字节数据用于下载
            csv_bytes = csv_buffer.getvalue()

            st.download_button(
                label="下载CSV文件",
                data=csv_bytes,
                file_name=f"{st.session_state['current_dataset']}_tunnel_download.csv",
                mime="text/csv; charset=gbk"
            )
            st.success(f"数据导出完成，共 {len(df_export)} 行，使用 {encoding_name.upper()} 编码")
        else:
            st.error("无法找到对应的查询语句")
    except Exception as e:
        import sys
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_number = exc_traceback.tb_lineno
        error_details = traceback.format_exc()

        print(f"错误发生在第 {line_number} 行")
        print(f"错误类型: {exc_type.__name__}")
        print(f"错误信息: {str(e)}")
        print("完整堆栈信息:")
        print(error_details)

        st.error(f"Tunnel下载失败: {str(e)} (发生在第 {line_number} 行)")
        with st.expander("查看详细错误信息"):
            st.code(error_details)