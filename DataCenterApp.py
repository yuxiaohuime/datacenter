from odps import ODPS
import streamlit as st
st.set_page_config(layout="wide", page_title="直播销售数据分析平台")
import pandas as pd
from openai import OpenAI

# 通过st.secrets管理ODPS认证信息
o = ODPS(st.secrets["odps"]["access_key_id"], 
         st.secrets["odps"]["access_key_secret"], 
         st.secrets["odps"]["project"],
         endpoint=st.secrets["odps"]["endpoint"])

def fetch_demo_data(dataset):
    """从ODPS获取直播数据"""
    if dataset=="六滋堂会员日历":
        reader = o.execute_sql("""
               SELECT  * FROM    yswy_ads.ads_lzt_customer_analysis_30_df WHERE   ds = MAX_PT('yswy_ads.ads_lzt_customer_analysis_30_df') and `门店`='宁波二店（联丰路店）' limit 100

           """).open_reader(tunnel=True, limit=False)

        # 转换为DataFrame
        data = []
        for record in reader:
            data.append(record.values)

        columns = [col.name for col in reader.schema.columns]
        return pd.DataFrame(data, columns=columns)



def get_lzt_shop():
    reader =    o.execute_sql("""select business_name  from yswy_dwd.yswy_dwd.dim_lzt_shop_df where ds=max_pt('yswy_dwd.yswy_dwd.dim_lzt_shop_df') group by business_name""").open_reader(tunnel=True, limit=False)
    return [record.values[0] for record in reader]


def analyze_data(data_str):
    """调用AI分析数据"""
    prompt = f"""
    以下是每日直播看播&下单数据，请分析并总结以下内容：
    1.给出客户画像
    2.给出运营优化建议
    数据如下：
    {data_str}
    """

    client = OpenAI(api_key=st.secrets["openai"]["api_key"],
                    base_url=st.secrets["openai"]["base_url"])

    response = client.chat.completions.create(
        model=st.secrets["openai"]["model"],
        messages=[{'role': 'user', 'content': prompt}],
        stream=True
    )

    return response




def main():
    st.title("直播销售数据分析平台")

    # 侧边栏配置
    st.sidebar.header("数据筛选")

    # 添加数据集选择选项
    dataset_option = st.sidebar.selectbox(
        "选择数据集",
        ("六滋堂会员日历")
    )
    secondary_filter = None
    if dataset_option == "六滋堂会员日历":
        # 缓存 shop_list 到 st.session_state 中
        if 'shop_list' not in st.session_state:
            with st.spinner("正在获取门店列表..."):
                st.session_state.shop_list = get_lzt_shop()
        shop_list = st.session_state.shop_list
        
        secondary_filter = st.sidebar.selectbox(
            "选择门店",
            ["全部门店"] + shop_list if shop_list else ["全部门店"]
        )

    if st.sidebar.button("获取最新数据"):
        with st.spinner("正在获取数据..."):
            # 根据选择的数据集获取不同数据
            if dataset_option == "六滋堂会员日历":
                if 'current_dataset' not in st.session_state:
                    df = fetch_demo_data("六滋堂会员日历")  # 使用现有函数
                    st.session_state['current_dataset'] = dataset_option
                    st.session_state['data'] = df
                base_query = """
                                SELECT  门店,用户昵称,用户手机号,积分,CONCAT_WS(',',添加的企微成员) 添加的企微成员,团长,最后一次消费时间,历史累计消费,日期,周,
                                        round(sum(看播时长),0) as 看播时长,round(sum(领取积分),0) as 领取积分,round(sum(金额),1) as 下单金额,累计看播时长,累计领取积分,累计金额
                                FROM    yswy_ads.ads_lzt_customer_analysis_30_df
                                WHERE   ds = MAX_PT('yswy_ads.ads_lzt_customer_analysis_30_df')
                                        and 累计看播时长<>0
                            """

                if secondary_filter and secondary_filter != "全部门店":
                    query_sql = base_query + f" AND `门店`='{secondary_filter}' GROUP BY 门店,用户昵称,用户手机号,积分,添加的企微成员,团长,最后一次消费时间,日期,周,累计看播时长,累计领取积分,累计金额,历史累计消费"
                else:
                    query_sql = base_query + " GROUP BY 门店,用户昵称,用户手机号,积分,添加的企微成员,团长,最后一次消费时间,日期,周,累计看播时长,累计领取积分,累计金额,历史累计消费"

                st.session_state['query_sql'] = query_sql


            elif dataset_option == "用户行为数据":
                # 在这里添加获取用户行为数据的函数
                df = []
            elif dataset_option == "商品销售数据":
                # 在这里添加获取商品销售数据的函数
                df = []

            st.success("数据获取成功！")

    # 显示数据
    if 'data' in st.session_state:
        df = st.session_state['data']
        current_dataset = st.session_state['current_dataset']
        st.subheader(f"展示样例数据 - {current_dataset}-点击完全导出数据获得对应全部数据")
        st.dataframe(df)


        # 数据导出功能 - 使用Tunnel Download
        if st.button("完全导出数据"):
            with st.spinner("正在通过Tunnel下载数据..."):
                #导出六滋堂日历数据
                from ExportData import export_lzt_date_by_shop
                export_lzt_date_by_shop(st,o)


        # AI分析
        if st.button("开始AI分析"):
            with st.spinner("AI正在分析数据..."):
                # 转换数据为字符串格式
                rows = []
                for _, row in df.iterrows():
                    rows.append("\t".join([str(item) for item in row]))
                data_str = "\n".join(rows)

                # 显示分析结果
                st.subheader("AI分析结果")
                response_placeholder = st.empty()
                full_response = ""

                for chunk in analyze_data(data_str):
                    if not chunk.choices:
                        continue
                    if chunk.choices[0].delta.content:
                        content = chunk.choices[0].delta.content
                        full_response += content
                        response_placeholder.markdown(full_response)

    else:
        st.info("请点击侧边栏'获取最新数据'按钮加载数据")


if __name__ == "__main__":
    main()
# streamlit run DataCenterApp.py