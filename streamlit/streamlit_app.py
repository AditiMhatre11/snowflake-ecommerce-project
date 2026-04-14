import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Ecommerce Delivery Dashboard", layout="wide")

session = get_active_session()

# Load data from Snowflake delivery views
sales_df = session.sql("SELECT * FROM DELIVERY.VW_SALES_DB ORDER BY INVOICE_DATE").to_pandas()
product_df = session.sql("SELECT * FROM DELIVERY.VW_PRODUCT_DB").to_pandas()
customer_df = session.sql("SELECT * FROM DELIVERY.VW_CUSTOMER_DB").to_pandas()

st.title("Ecommerce Analytics Dashboard")

tab1, tab2, tab3 = st.tabs(["Sales Overview", "Product Performance", "Customer Insights"])

with tab1:
    st.subheader("Sales Overview")

    total_revenue = float(sales_df["TOTAL_REVENUE"].sum()) if not sales_df.empty else 0
    total_orders = int(sales_df["TOTAL_ORDER"].sum()) if not sales_df.empty else 0
    total_net_sales = float(sales_df["NET_SALES"].sum()) if not sales_df.empty else 0
    total_returns = float(sales_df["RETURNED_VALUE"].sum()) if not sales_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue", f"${total_revenue/1e6:,.1f}M")
    c2.metric("Total Orders", f"{total_orders:,}")
    c3.metric("Net Sales", f"${total_net_sales/1e6:,.1f}M")
    c4.metric("Returned Value", f"${total_returns/1e6:,.1f}M")
    
    chart_data = sales_df.copy()
    chart_data["INVOICE_DATE"] = pd.to_datetime(chart_data["INVOICE_DATE"])
    chart_data = chart_data[chart_data["INVOICE_DATE"] <= "2016-12-31"]
    chart_data = chart_data.sort_values("INVOICE_DATE")
    chart_data = chart_data.set_index("INVOICE_DATE")[["TOTAL_REVENUE", "NET_SALES"]].resample("M").sum()

    chart_data["RETURN_IMPACT"] = chart_data["TOTAL_REVENUE"] - chart_data["NET_SALES"]

    st.line_chart(chart_data)

    st.dataframe(sales_df, use_container_width=True)

with tab2:
    st.subheader("Product Performance")

    top_n = st.slider("Top N Products", min_value=5, max_value=20, value=10)

    # Top products (no change)
    top_products = product_df.sort_values("TOTAL_REVENUE", ascending=False).head(top_n)

    filtered_returns = product_df[
        ~product_df["DESCRIPTION"].str.contains(
            "fee|charge|discount|manual", case=False, na=False
        )
    ]

    top_returns = filtered_returns.sort_values("RETURN_VALUE", ascending=False).head(top_n)

    st.write("Top Products by Revenue")
    st.bar_chart(top_products.set_index("DESCRIPTION")["TOTAL_REVENUE"])

    st.write("Most Returned Products")
    st.bar_chart(top_returns.set_index("DESCRIPTION")["RETURN_VALUE"])

    # -------------------------------
    # Product details table
    # -------------------------------
    st.write("Top Product Details")
    st.dataframe(
        top_products[[
            "DESCRIPTION",
            "TOTAL_REVENUE",
            "TOTAL_ORDERS",
            "TOTAL_QUANTITY_SOLD"
        ]],
        use_container_width=True
    )


import altair as alt
import pandas as pd

with tab3:
    st.subheader("Customer Insights")
    st.write("Customer Behavior: Orders vs Spend")

    customer_chart_df = customer_df.copy()

    # Clean types
    customer_chart_df["CUSTOMER_ID"] = customer_chart_df["CUSTOMER_ID"].astype(str)
    customer_chart_df["TOTAL_ORDER"] = pd.to_numeric(customer_chart_df["TOTAL_ORDER"], errors="coerce")
    customer_chart_df["TOTAL_SPENT"] = pd.to_numeric(customer_chart_df["TOTAL_SPENT"], errors="coerce")

        # Create order groups
    max_orders = customer_chart_df["TOTAL_ORDER"].max()
    customer_chart_df["ORDER_GROUP"] = pd.cut(
        customer_chart_df["TOTAL_ORDER"],
        bins=[0, 20, 50, max_orders],
        labels=["Low Orders", "Medium Orders", "High Orders"],
        include_lowest=True
    )

    chart = alt.Chart(customer_chart_df).mark_circle(size=120, opacity=0.8).encode(
        x=alt.X("TOTAL_ORDER:Q", title="Total Order", scale=alt.Scale(domain=[0, 140])),
        y=alt.Y("TOTAL_SPENT:Q", title="Total Spend"),
        color=alt.Color("ORDER_GROUP:N", title="Order Group"),
        tooltip=["CUSTOMER_ID", "TOTAL_ORDER", "TOTAL_SPENT", "ORDER_GROUP"]
    ).properties(
        height=450
    ).interactive()

    st.altair_chart(chart, use_container_width=True)

    st.write(f"Rows plotted: {len(customer_chart_df)}")
    st.dataframe(
        customer_chart_df[["CUSTOMER_ID", "TOTAL_ORDER", "TOTAL_SPENT", "ORDER_GROUP"]],
        use_container_width=True
    )
