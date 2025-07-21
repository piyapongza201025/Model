# -- ✅ รวมทุกบริษัทครบ 8 บริษัทใน query เดียว
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from flask import Flask, jsonify
import numpy as np

# สร้าง connection string
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=192.168.75.203;'
    'DATABASE=DB_Center_Coldchain;'
    'UID=piyapong.c;'
    'PWD=D@0ne!#*;'
    'TrustServerCertificate=yes;'
)


app = Flask(__name__)

# เขียน query PCS
query = """
WITH AllCompanies AS (

    -- PCS
    SELECT
        'PCS' AS Company,
        m_cus.customerid,
        CONCAT('PCS', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM PCS_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM PCS_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM PCS_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM PCS_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM PCS_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

    UNION ALL

    -- PACA
    SELECT
        'PACA' AS Company,
        m_cus.customerid,
        CONCAT('PACA', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM PACA_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM PACA_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM PACA_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM PACA_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM PACA_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

    UNION ALL

    -- PACM
    SELECT
        'PACM' AS Company,
        m_cus.customerid,
        CONCAT('PACM', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM PACM_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM PACM_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM PACM_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM PACM_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM PACM_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

    UNION ALL

    -- PACT
    SELECT
        'PACT' AS Company,
        m_cus.customerid,
        CONCAT('PACT', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM PACT_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM PACT_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM PACT_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM PACT_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM PACT_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

    UNION ALL

    -- PACR
    SELECT
        'PACR' AS Company,
        m_cus.customerid,
        CONCAT('PACR', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM PACR_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM PACR_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM PACR_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM PACR_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM PACR_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

    UNION ALL

    -- PACS
    SELECT
        'PACS' AS Company,
        m_cus.customerid,
        CONCAT('PACS', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM PACS_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM PACS_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM PACS_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM PACS_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM PACS_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

    UNION ALL

    -- PACJ
    SELECT
        'PACJ' AS Company,
        m_cus.customerid,
        CONCAT('PACJ', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM JPAC_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM JPAC_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM JPAC_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM JPAC_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM JPAC_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

		UNION ALL

		SELECT
        'PACK' AS Company,
        m_cus.customerid,
        CONCAT('PACK', '-', m_cus.customerid) AS key_customer,
        m_cus.CustomerName,
        fd.FirstDate,
        sb.Stock_Balance_ton,
        ISNULL(i.Recency_receive, NULL) AS Recency_receive,
        ISNULL(o.Recency_dispatch, NULL) AS Recency_dispatch,
        ISNULL(i.Frequency_receive, 0) AS Frequency_receive,
        ISNULL(o.Frequency_dispatch, 0) AS Frequency_dispatch,
        ISNULL(i.Monetary_receive, 0) AS Monetary_receive,
        ISNULL(o.Monetary_dispatch, 0) AS Monetary_dispatch
    FROM JPK_CCMS_CUSTOMER m_cus
    LEFT JOIN (
        SELECT customerid, MAX(SReceiveDate) AS Recency_receive,
               COUNT(DISTINCT LotNo) AS Frequency_receive,
               SUM(ConfirmWeight) / 1000 AS Monetary_receive
        FROM JPK_CCMS_VOLUME_IN
        GROUP BY customerid
    ) i ON m_cus.CustomerId = i.customerid
    LEFT JOIN (
        SELECT customerid, MAX(SDispatchDate) AS Recency_dispatch,
               COUNT(DISTINCT LotNo) AS Frequency_dispatch,
               SUM(ConfirmWeight) / 1000 AS Monetary_dispatch
        FROM JPK_CCMS_VOLUME_OUT
        GROUP BY customerid
    ) o ON m_cus.CustomerId = o.customerid
    LEFT JOIN (
        SELECT CustomerID, MIN(SReceiveDate) AS FirstDate
        FROM JPK_CCMS_VOLUME_IN
        GROUP BY CustomerID
    ) fd ON m_cus.CustomerId = fd.CustomerID
    LEFT JOIN (
        SELECT CustomerID, SUM(BalanceWeight)/1000 AS Stock_Balance_ton
        FROM JPK_CCMS_FACT_USINGSPACE
        GROUP BY CustomerID
    ) sb ON m_cus.CustomerId = sb.CustomerID
    WHERE i.Recency_receive IS NOT NULL

)

-- Join กับ Product_type
SELECT a.*, pt.producttype
FROM AllCompanies a
LEFT JOIN (
    SELECT *, CONCAT(company,'-',cus_custid) AS key_customer
    FROM CCMS_ColdChain_ProductType
) pt ON a.key_customer = pt.key_customer;
"""

# ใช้ cursor รัน
cursor = conn.cursor()
cursor.execute(query)

# ดึง column name
columns = [column[0] for column in cursor.description]

# ดึงผลลัพธ์
data = cursor.fetchall()

# แปลงเป็น DataFrame
df = pd.DataFrame.from_records(data, columns=columns)


# ✅ วันอ้างอิง
today = pd.Timestamp.today().normalize()

# ✅ หาค่า Recency
df['Recency_receive_1'] = (today - df['Recency_receive']).dt.days
df['Recency_dispatch_1'] = (today - df['Recency_dispatch']).dt.days

# ✅ แบ่งกลุ่ม R_Score ตามช่วงที่กำหนด
def r_score(days):
    if days < 90:
        return 4
    elif days < 180:
        return 3
    elif days < 360:
        return 2
    else:
        return 1

df['R_Score_receive'] = df['Recency_receive_1'].apply(r_score)
df['R_Score_dispatch'] = df['Recency_dispatch_1'].apply(r_score)

# ✅ แบ่ง F และ M Score ตาม Percentile
def percentile_score(series):
    # หาค่าตาม percentile
    q25 = series.quantile(0.25)
    q50 = series.quantile(0.50)
    q75 = series.quantile(0.75)

    def score(x):
        if x <= q25:
            return 1
        elif x <= q50:
            return 2
        elif x <= q75:
            return 3
        else:
            return 4
    return series.apply(score)

df['Monetary_receive'] = df['Monetary_receive'].astype(float)

df['F_Score_receive'] = percentile_score(df['Frequency_receive'])
df['F_Score_dispatch'] = percentile_score(df['Frequency_dispatch'])
df['M_Score_receive'] = percentile_score(df['Monetary_receive'])
df['M_Score_dispatch'] = percentile_score(df['Monetary_dispatch'])

# ✅ รวม RFM Score
df['RFM_Score'] = (
    df['R_Score_receive'].astype(str) +
    df['R_Score_dispatch'].astype(str) +
    df['F_Score_receive'].astype(str) +
    df['F_Score_dispatch'].astype(str) +
    df['M_Score_receive'].astype(str) +
    df['M_Score_dispatch'].astype(str)
)


def assign_segment(row):
    r_receive = row['R_Score_receive']
    r_dispatch = row['R_Score_dispatch']
    f_receive = row['F_Score_receive']
    f_dispatch = row['F_Score_dispatch']
    m_receive = row['M_Score_receive']
    m_dispatch = row['M_Score_dispatch']

    # 🎯 Champions
    if r_receive >= 3 and r_dispatch >= 3 and f_receive >= 3 and f_dispatch >= 3 and m_receive >= 3 and m_dispatch >= 3:
        return 'Champions'

    # 🫶 Loyal Customers
    elif r_receive >= 3 and r_dispatch >= 3 and (f_receive >= 2 or f_dispatch >= 2):
        return 'Loyal Customers'

    # ⚠️ At Risk
    elif r_receive <= 2 and r_dispatch <= 2 and (f_receive >= 3 or m_receive >= 3):
        return 'At Risk'

    # ❌ Lost Customers
    elif r_receive == 1 and r_dispatch == 1:
        return 'Lost Customers'

    # 🌱 Potential
    else:
        return 'Potential'

# ✅ สร้างคอลัมน์ Segment
df['Segment'] = df.apply(assign_segment, axis=1)

for col in df.select_dtypes(include=[np.datetime64, 'datetime', 'object']).columns:
    if df[col].isnull().any():
        df[col] = df[col].replace({np.nan: None})


@app.route('/api/all_rfm_data', methods=['GET'])
def get_all_rfm_data():
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, debug=True)
