
from flask import Flask, jsonify
import pyodbc
import pandas as pd

app = Flask(__name__)

# ✅ Function: เชื่อมต่อฐานข้อมูล + Query ข้อมูล + ทำ RFM
def get_customer_rfm():
    # เชื่อมต่อ SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.75.203;'
        'DATABASE=DB_Center_Coldchain;'
        'UID=piyapong.c;'
        'PWD=D@0ne!#*;'
        'TrustServerCertificate=yes;'
    )

    # SQL Query (ตาม code ของคุณ)
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

    print(df.head())

    return df

# ✅ Route API: /customer-rfm
@app.route('/customer-rfm', methods=['GET'])
def customer_rfm():
    try:
        df = get_customer_rfm()
        result = df.to_dict(orient='records')  # แปลง DataFrame เป็น JSON
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Start API Server
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
