import streamlit as st
import pandas as pd
import io
import traceback
from TushareData import query_stocks_with_double_tail_number

# 设置页面为宽屏模式
st.set_page_config(
    page_title="股票数据查询平台",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 页面标题
st.title("📈 股票数据查询平台")
st.markdown("---")

# 定义数据集类型
DATASET_TYPES = {
    "双尾数股票": {
        "function": query_stocks_with_double_tail_number,
        "description": "查询最近N个交易日内出现最低价为双尾数（如1.33）的股票",
        "columns": ["name", "ts_code", "trade_date", "low"]
    }
    # 后续可以在这里添加更多数据集类型
    # "涨停股票": {
    #     "function": query_limit_up_stocks,
    #     "description": "查询近期涨停的股票",
    #     "columns": ["name", "ts_code", "trade_date", "close", "pct_chg"]
    # },
    # "跌幅股票": {
    #     "function": query_limit_down_stocks,
    #     "description": "查询近期跌幅较大的股票",
    #     "columns": ["name", "ts_code", "trade_date", "close", "pct_chg"]
    # }
}

# 侧边栏设置
st.sidebar.header("⚙️ 查询设置")

# 选择数据集类型
dataset_type = st.sidebar.selectbox(
    "选择数据集类型",
    options=list(DATASET_TYPES.keys()),
    help="选择要查询的数据集类型"
)

# 显示当前数据集的描述
st.sidebar.info(DATASET_TYPES[dataset_type]["description"])

# 根据不同数据集类型设置参数
if dataset_type == "双尾数股票":
    days = st.sidebar.slider("选择查询天数", min_value=1, max_value=180, value=180,
                            help="查询最近N个交易日内的双尾数股票")

# 查询按钮
if st.sidebar.button("🔍 查询数据", type="primary"):
    with st.spinner("正在查询数据，请稍候..."):
        try:
            # 根据选择的数据集类型调用相应函数
            if dataset_type == "双尾数股票":
                df_result = DATASET_TYPES[dataset_type]["function"](days=days)
            else:
                # 其他数据集类型的调用方式
                df_result = DATASET_TYPES[dataset_type]["function"]()
            
            if df_result is not None and not df_result.empty:
                # 保存数据到session_state
                st.session_state['current_data'] = df_result
                st.session_state['dataset_type'] = dataset_type
                st.session_state['query_executed'] = True
                st.success(f"查询完成！共找到 {len(df_result)} 条记录")
            elif df_result is not None and df_result.empty:
                st.session_state['current_data'] = df_result
                st.session_state['dataset_type'] = dataset_type
                st.session_state['query_executed'] = True
                st.warning("查询完成，但未找到符合条件的数据")
            else:
                st.session_state['query_executed'] = False
                st.error("查询过程中发生错误，请检查网络连接或数据库配置")
        except Exception as e:
            st.session_state['query_executed'] = False
            st.error(f"查询过程中发生异常: {str(e)}")
            st.text("详细错误信息:")
            st.code(traceback.format_exc())

# 显示查询结果
if 'query_executed' in st.session_state and st.session_state['query_executed']:
    df_data = st.session_state['current_data']
    dataset_type = st.session_state['dataset_type']
    
    if df_data is not None and not df_data.empty:
        # 显示数据统计信息
        st.subheader("📊 查询结果统计")
        col1, col2 = st.columns(2)
        col1.metric("记录总数", len(df_data))
        
        if 'ts_code' in df_data.columns:
            col2.metric("股票数量", df_data['ts_code'].nunique())
        
        st.markdown("---")
        
        # 显示数据表格
        st.subheader(f"📋 {dataset_type}详情")
        
        # 使用可展开的数据表格
        with st.expander("点击查看详细数据", expanded=True):
            # 设置表格高度和宽度
            st.dataframe(
                df_data,
                use_container_width=True,
                height=500
            )
        
        # 下载功能
        st.subheader("💾 数据导出")
        
        # 提供多种导出选项
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV格式下载
            def convert_df_to_csv(df):
                # 创建字节缓冲区
                buffer = io.BytesIO()
                # 写入DataFrame到缓冲区
                df.to_csv(buffer, index=False, encoding='utf-8-sig')
                # 重置缓冲区指针
                buffer.seek(0)
                return buffer.getvalue()
            
            csv_data = convert_df_to_csv(df_data)
            st.download_button(
                label="📥 下载CSV格式",
                data=csv_data,
                file_name=f"{dataset_type}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="下载CSV格式文件，适用于Excel等表格软件"
            )
        
        with col2:
            # Excel格式下载
            def convert_df_to_excel(df):
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name=dataset_type)
                buffer.seek(0)
                return buffer.getvalue()
            
            if st.button("📊 生成Excel格式"):
                with st.spinner("正在生成Excel文件..."):
                    try:
                        excel_data = convert_df_to_excel(df_data)
                        st.download_button(
                            label="📥 下载Excel格式",
                            data=excel_data,
                            file_name=f"{dataset_type}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="下载Excel格式文件"
                        )
                    except Exception as e:
                        st.error(f"生成Excel文件时出错: {e}")
        
        # 根据数据集类型显示相应的数据分析
        st.subheader("📈 数据分析")
        
        # 只对包含特定列的数据进行分析
        if 'trade_date' in df_data.columns:
            st.write("**按交易日期统计:**")
            date_counts = df_data['trade_date'].value_counts().sort_index()
            st.bar_chart(date_counts)
        
        if 'name' in df_data.columns:
            st.write("**按股票统计 (前20只):**")
            stock_counts = df_data['name'].value_counts().head(20)
            st.bar_chart(stock_counts)
        
        # if 'low' in df_data.columns:
        #     st.write("**最低价分布:**")
        #     st.line_chart(df_data.groupby('trade_date')['low'].mean())
        
    else:
        st.info("暂无数据可显示")
elif 'query_executed' in st.session_state and not st.session_state['query_executed']:
    st.info("请点击侧边栏的查询按钮获取数据")
else:
    # 首页说明
    st.info("ℹ️ 请在左侧边栏选择数据集类型并设置查询参数，然后点击查询按钮")
    
    st.markdown("### 当前支持的数据集类型")
    for name, info in DATASET_TYPES.items():
        st.markdown(f"- **{name}**: {info['description']}")
    
    st.markdown("### 💡 使用说明")
    st.markdown("""
    1. 在左侧边栏选择要查询的数据集类型
    2. 根据所选数据集类型设置相应参数
    3. 点击"查询数据"按钮获取数据
    4. 查看查询结果和数据分析
    5. 可导出数据为CSV或Excel格式
    """)
    
    st.markdown("### 🚀 平台特点")
    st.markdown("""
    - 支持多种股票数据集查询
    - 美观的数据展示界面
    - 多种数据导出格式
    - 内置数据分析图表
    - 易于扩展的架构设计
    """)


# 页面底部
st.markdown("---")
st.caption("股票数据查询平台 | 作者：yuxiaohui")