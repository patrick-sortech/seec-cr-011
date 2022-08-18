import br.com.sortech.utils.configurations.database.ConnectionManager;

    import java.sql.*;
    import java.time.LocalDateTime;
    import java.util.HashMap;
    import java.util.HashSet;
    import java.util.Iterator;
    import java.util.Properties;


public class TesteOracle {

    static long BATCH_SIZE = 2;
    static HashMap<Long, HashSet<Long>> customerAccountHashMap = new HashMap<Long, HashSet<Long>>();

    static PreparedStatement pstmtAggAccount;
    static PreparedStatement pstmtAggCustomer;

    static final String sqlQueryAggCustomer = "select /*+ all_rows */\n" +
            " tc.customer_id\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_atual\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_final_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_final\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_orig_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_original\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_atu_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_corrigido\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN 1\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_DEBITOS_vencidos\n" +
            "    , to_date(MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCIDA_PRIMEIRA\n" +
            "    , to_date(MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCIDA_ULTIMA\n" +
            "    \n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_atual\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_final_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_final\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_orig_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_original\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_atu_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_corrigido\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN 1\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS  tt_DEBITOS_vencER\n" +
            "    , to_date(MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCER_PRIMEIRA\n" +
            "    , to_date(MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCER_ULTIMA\n" +
            "\n" +
            "  , nvl(SUM(xb.vl_divida_atual),0) AS tt_saldo_atual\n" +
            "    , nvl(SUM( xb.vl_final_lancamento),0) AS tt_saldo_final\n" +
            "    , nvl(SUM(xb.vl_orig_lancamento),0)  AS tt_saldo_original\n" +
            "    , nvl(SUM( xb.vl_atu_lancamento),0)  AS tt_saldo_corrigido\n" +
            "    , sum(\n" +
            "      CASE\n" +
            "        WHEN bill.bill_due_amount > 0 THEN 1\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS TT_DEBITOS_ABERTOS\n" +
            "    , MIN(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) AS DT_VENC_PRIMEIRA \n" +
            "   , MAX(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) AS DT_VENC_ULTIMA\n" +
            "    \n" +
            "   , MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCIDO_MAIOR_ABERTO\n" +
            "    , MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCIDO_MENOR_ABERTO\n" +
            "    \n" +
            "    \n" +
            "    , MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCER_MAIOR_ABERTO\n" +
            "    , MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCER_MENOR_ABERTO\n" +
            "    \n" +
            "    \n" +
            "    , NVL(MAX(xb.vl_divida_atual),0) AS VL_DEBITO_MAIOR_ABERTO\n" +
            "    , NVL(MIN(xb.vl_divida_atual),0) AS VL_DEBITO_MENOR_ABERTO\n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    , nvl(MAX(\n" +
            "      CASE\n" +
            "        WHEN monitor.oldest_bill_id = - 1 THEN - 99999\n" +
            "        ELSE trunc(trunc(SYSDATE) - trunc(monitor.oldest_bill_due_date))\n" +
            "      END\n" +
            "    ), - 99999) AS dias_de_mora_monitoring\n" +
            "    , nvl(trunc(SYSDATE - MIN(trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')))), - 99999) AS dias_de_mora_bill\n" +
            "   \n" +
            "  FROM\n" +
            "    \n" +
            "    ics_it_t_customer tc\n" +
            "    LEFT JOIN ics_it_t_bill bill\n" +
            "    ON tc.customer_id = bill.customer_id\n" +
            "  --     AND bill.bill_status_id = 'OPEN'\n" +
            "  and bill_due_amount > 0\n" +
            "       AND bill.bill_canceled = 'N'\n" +
            "    LEFT JOIN ics_it_x_bill xb\n" +
            "        ON bill.bill_id = xb.bill_id\n" +
            "    LEFT JOIN ics_cm_l_customer_monitoring monitor\n" +
            "    ON monitor.customer_id = tc.customer_id  \n" +
            "  WHERE 1=1\n" +
            "  and tc.customer_id = ?\n" +
            "  GROUP BY\n" +
            "    tc.customer_id\n";

    static final String sqlQueryAggAccount = "select /*+ all_rows */\n" +
            " ta.customer_id\n" +
            " , ta.account_id\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_atual\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_final_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_final\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_orig_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_original\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_atu_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencido_corrigido\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN 1\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_DEBITOS_vencidos\n" +
            "    , to_date(MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCIDA_PRIMEIRA\n" +
            "    ,to_date( MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCIDA_ULTIMA\n" +
            "    \n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_atual\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_final_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_final\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_orig_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_original\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_atu_lancamento\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS tt_saldo_vencer_corrigido\n" +
            "    , SUM(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN 1\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS  tt_DEBITOS_vencER\n" +
            "    , to_date(MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCER_PRIMEIRA\n" +
            "    , to_date(MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.dt_vencimento_atual\n" +
            "      END\n" +
            "    ),'YYYYMMDD') AS DT_VENC_VENCER_ULTIMA\n" +
            "\n" +
            "  , nvl(SUM(xb.vl_divida_atual),0) AS tt_saldo_atual\n" +
            "    , nvl(SUM( xb.vl_final_lancamento),0) AS tt_saldo_final\n" +
            "    , nvl(SUM(xb.vl_orig_lancamento),0)  AS tt_saldo_original\n" +
            "    , nvl(SUM( xb.vl_atu_lancamento),0)  AS tt_saldo_corrigido\n" +
            "    , sum(\n" +
            "      CASE\n" +
            "        WHEN bill.bill_due_amount > 0 THEN 1\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS TT_DEBITOS_ABERTOS\n" +
            "    , MIN(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) AS DT_VENC_PRIMEIRA \n" +
            "   , MAX(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) AS DT_VENC_ULTIMA\n" +
            "    \n" +
            "   , MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCIDO_MAIOR_ABERTO\n" +
            "    , MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) < trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCIDO_MENOR_ABERTO\n" +
            "    \n" +
            "    \n" +
            "    , MAX(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCER_MAIOR_ABERTO\n" +
            "    , MIN(\n" +
            "      CASE\n" +
            "        WHEN trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')) >= trunc(SYSDATE) THEN xb.vl_divida_atual\n" +
            "        ELSE 0\n" +
            "      END\n" +
            "    ) AS VL_DEBITO_VENCER_MENOR_ABERTO\n" +
            "    \n" +
            "    \n" +
            "    , NVL(MAX(xb.vl_divida_atual),0) AS VL_DEBITO_MAIOR_ABERTO\n" +
            "    , NVL(MIN(xb.vl_divida_atual),0) AS VL_DEBITO_MENOR_ABERTO\n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    \n" +
            "    , nvl(MAX(\n" +
            "      CASE\n" +
            "        WHEN monitor.oldest_bill_id = - 1 THEN - 99999\n" +
            "        ELSE trunc(trunc(SYSDATE) - trunc(monitor.oldest_bill_due_date))\n" +
            "      END\n" +
            "    ), - 99999) AS dias_de_mora_monitoring\n" +
            "    , nvl(trunc(SYSDATE - MIN(trunc(to_date(xb.dt_vencimento_atual,'YYYYMMDD')))), - 99999) AS dias_de_mora_bill\n" +
            "   \n" +
            "  FROM\n" +
            "    \n" +
            "    ics_it_t_customer tc \n" +
            "    left join ics_it_t_account ta on tc.customer_id = ta.customer_id\n" +
            "    LEFT JOIN ics_it_t_bill bill ON tc.customer_id = bill.customer_id and ta.account_id = bill.account_id\n" +
            "  --     AND bill.bill_status_id = 'OPEN'\n" +
            "  and bill_due_amount > 0\n" +
            "       AND bill.bill_canceled = 'N'\n" +
            "    LEFT JOIN ics_it_x_bill xb\n" +
            "        ON bill.bill_id = xb.bill_id\n" +
            "    LEFT JOIN ics_cm_l_account_monitoring monitor\n" +
            "    ON monitor.customer_id = ta.customer_id  and monitor.account_id = ta.account_id\n" +
            "  WHERE 1=1\n" +
            "  and tc.customer_id = ? and ta.account_id = ?\n" +
            "  GROUP BY\n" +
            "    ta.customer_id, ta.account_id\n";


    static PreparedStatement pstmtUpdateAggAccount;
    static PreparedStatement pstmtUpdateAggCustomer;

    static final String sqlUpdateAggAccount = "UPDATE ICS_IT_X_ACCOUNT " +
            "SET TT_SALDO_VENCIDO_ATUAL = ?, TT_SALDO_VENCIDO_FINAL = ?, TT_SALDO_VENCIDO_ORIGINAL = ?, TT_SALDO_VENCIDO_CORRIGIDO = ?" +
            ", TT_DEBITOS_VENCIDOS = ?, DT_VENC_VENCIDA_PRIMEIRA = ?, DT_VENC_VENCIDA_ULTIMA = ? " +
            ", TT_SALDO_VENCER_ATUAL = ?, TT_SALDO_VENCER_FINAL = ?, TT_SALDO_VENCER_ORIGINAL = ?, TT_SALDO_VENCER_CORRIGIDO = ? " +
            ", TT_DEBITOS_VENCER = ?, DT_VENC_VENCER_PRIMEIRA = ?, DT_VENC_VENCER_ULTIMA = ? " +
            ", TT_SALDO_ATUAL = ?, TT_SALDO_FINAL = ?, TT_SALDO_ORIGINAL = ?, TT_SALDO_CORRIGIDO = ?" +
            ", TT_DEBITOS_ABERTOS = ?, DT_VENC_PRIMEIRA = ?, DT_VENC_ULTIMA = ? " +
            ", VL_DEBITO_VENCIDO_MAIOR_ABERTO = ? , VL_DEBITO_VENCIDO_MENOR_ABERTO = ? " +
            ", VL_DEBITO_VENCER_MAIOR_ABERTO = ? , VL_DEBITO_VENCER_MENOR_ABERTO = ? " +
            ", VL_DEBITO_MAIOR_ABERTO = ? , VL_DEBITO_MENOR_ABERTO = ? " +
            ", DIAS_DE_MORA_MONITORING = ?, DIAS_DE_MORA_BILL = ? " +
            " WHERE CUSTOMER_ID = ? AND ACCOUNT_ID = ?";

    static final String sqlUpdateAggCustomer = "UPDATE ICS_IT_X_CUSTOMER " +
            " SET TT_SALDO_VENCIDO_ATUAL = ?, TT_SALDO_VENCIDO_FINAL = ?, TT_SALDO_VENCIDO_ORIGINAL = ?, TT_SALDO_VENCIDO_CORRIGIDO = ? " +
            ", TT_DEBITOS_VENCIDOS = ?, DT_VENC_VENCIDA_PRIMEIRA = ?, DT_VENC_VENCIDA_ULTIMA = ? " +
            ", TT_SALDO_VENCER_ATUAL = ?, TT_SALDO_VENCER_FINAL = ?, TT_SALDO_VENCER_ORIGINAL = ?, TT_SALDO_VENCER_CORRIGIDO = ? " +
            ", TT_DEBITOS_VENCER = ?, DT_VENC_VENCER_PRIMEIRA = ?, DT_VENC_VENCER_ULTIMA = ? " +
            ", TT_SALDO_ATUAL = ?, TT_SALDO_FINAL = ?, TT_SALDO_ORIGINAL = ?, TT_SALDO_CORRIGIDO = ? " +
            ", TT_DEBITOS_ABERTOS = ?, DT_VENC_PRIMEIRA = ?, DT_VENC_ULTIMA = ? " +
            ", VL_DEBITO_VENCIDO_MAIOR_ABERTO = ? , VL_DEBITO_VENCIDO_MENOR_ABERTO = ? " +
            ", VL_DEBITO_VENCER_MAIOR_ABERTO = ? , VL_DEBITO_VENCER_MENOR_ABERTO = ? " +
            ", VL_DEBITO_MAIOR_ABERTO = ? , VL_DEBITO_MENOR_ABERTO = ? " +
            ", DIAS_DE_MORA_MONITORING = ? , DIAS_DE_MORA_BILL = ? " +
            " WHERE CUSTOMER_ID = ? ";



    static public void main (String[] args) throws Exception {

        //PropertiesLoader propLoader = new PropertiesLoader("database.properties");
        //propLoader.listProperties();

        Properties props = new Properties();
        props.load(App.class.getClassLoader().getResourceAsStream("database.properties"));
        props.list(System.out);

        ConnectionManager connMgr = new ConnectionManager(props.getProperty("datasource.driver-class-name")
                , props.getProperty("datasource.url"), props.getProperty("datasource.username"), props.getProperty("datasource.password"));

        Connection conn  = connMgr.getConnection();
        conn.setAutoCommit(false);

        getEvents(conn, 1);

    }


    static public void getEvents(Connection conn, long eventCode) throws SQLException {
        conn.setAutoCommit(false);
        String sqlQuery = "SELECT ID_EVENT, CO_EVENT, CUSTOMER_ID, ACCOUNT_ID, BILL_ID, TX_REQUEST_DATA, DT_REQUEST, ST_REQUEST " +
                "FROM RCT_EVT_E_EVENT_REQUEST " +
                "WHERE CO_EVENT = 1 AND ST_REQUEST = 1";

        String updateEventTableQuery = "UPDATE RCT_EVT_E_EVENT_REQUEST " +
                " SET ST_REQUEST = ? , DT_REQ_EXEC = ?, ST_REQ_EXEC =? " +
                " WHERE ID_EVENT = ? ";

        long recCounter = 0;

        try {
            PreparedStatement pstmt = conn.prepareStatement(sqlQuery);
            PreparedStatement pstmtUpdateEventTable = conn.prepareStatement(updateEventTableQuery);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                billEventHandler(rs, pstmtUpdateEventTable);
                setCustomerAccountData(rs);
                recCounter++;
                if (recCounter % BATCH_SIZE == 0) {
                    eventUpdate(pstmtUpdateEventTable);
                    accountEventHandler(conn);
                    customerEventHandler(conn);
                    conn.commit();
                    recCounter =0;
                    customerAccountHashMap = new HashMap<>();
                }

            }

            eventUpdate(pstmtUpdateEventTable);
            accountEventHandler(conn);
            customerEventHandler(conn);
            conn.commit();
            recCounter =0;
            customerAccountHashMap = new HashMap<>();


        } catch (Exception ex) {
            conn.rollback();
            ex.printStackTrace();
        }

    }

    static public void setCustomerAccountData(ResultSet rs) throws Exception{
        HashSet<Long> accountsList = null;
        accountsList = customerAccountHashMap.get(rs.getLong("CUSTOMER_ID"));

        if (accountsList == null) {
            accountsList = new HashSet<Long>();
        }
        accountsList.add(rs.getLong("ACCOUNT_ID"));
        customerAccountHashMap.put(rs.getLong("CUSTOMER_ID"), accountsList);
    }

    static public void billEventHandler(ResultSet rs, PreparedStatement pstmt) throws Exception {
        printRow(rs);
        pstmt.setLong(1, 0);
        pstmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        pstmt.setLong(3, 0);
        pstmt.setLong(4, rs.getLong("ID_EVENT"));
        pstmt.addBatch();
    }

    static public void  eventUpdate(PreparedStatement pstmt) throws Exception {
        pstmt.executeBatch();

    }

    static public void accountEventHandler(Connection conn) throws Exception {
        ResultSet rsSelAccount;
        long customerId;
        long accountId;
        HashSet<Long> accounts;

        pstmtAggAccount = conn.prepareStatement(sqlQueryAggAccount);
        pstmtUpdateAggAccount = conn.prepareStatement(sqlUpdateAggAccount);

        Iterator itCustomer = customerAccountHashMap.keySet().iterator();
        Iterator itAccounts;

        try {

            customerAccountHashMap.entrySet().forEach(entry -> {
                System.out.println("Customer Id: " + entry.getKey() + " Account Ids " + entry.getValue().toString());
            });

            while (itCustomer.hasNext()) {
                customerId = (Long) itCustomer.next();

                itAccounts = customerAccountHashMap.get(customerId).iterator();

                while (itAccounts.hasNext()) {

                    accountId = (Long) itAccounts.next();
                    System.out.println("Customer Id: " + customerId + " Account Id " + accountId);

                    pstmtAggAccount.setLong(1, customerId);
                    pstmtAggAccount.setLong(2, accountId);
                    rsSelAccount = pstmtAggAccount.executeQuery();

                    while (rsSelAccount.next()) {
                        for (int i = 1; i <= 31; i++) {
                            System.out.println("i = " + i + ": " + rsSelAccount.getString(i));
                        }

                        // valores vencido
                        pstmtUpdateAggAccount.setDouble(1, rsSelAccount.getDouble(3));
                        pstmtUpdateAggAccount.setDouble(2, rsSelAccount.getDouble(4));
                        pstmtUpdateAggAccount.setDouble(3, rsSelAccount.getDouble(5));
                        pstmtUpdateAggAccount.setDouble(4, rsSelAccount.getDouble(6));
                        pstmtUpdateAggAccount.setLong(5, rsSelAccount.getLong(7));
                        pstmtUpdateAggAccount.setDate(6, rsSelAccount.getDate(8));
                        pstmtUpdateAggAccount.setDate(7, rsSelAccount.getDate(9));
                        // valores vencer
                        pstmtUpdateAggAccount.setDouble(8, rsSelAccount.getDouble(10));
                        pstmtUpdateAggAccount.setDouble(9, rsSelAccount.getDouble(11));
                        pstmtUpdateAggAccount.setDouble(10, rsSelAccount.getDouble(12));
                        pstmtUpdateAggAccount.setDouble(11, rsSelAccount.getDouble(13));
                        pstmtUpdateAggAccount.setLong(12, rsSelAccount.getLong(14));
                        pstmtUpdateAggAccount.setDate(13, rsSelAccount.getDate(15));
                        pstmtUpdateAggAccount.setDate(14, rsSelAccount.getDate(16));
                        // valores totais
                        pstmtUpdateAggAccount.setDouble(15, rsSelAccount.getDouble(17));
                        pstmtUpdateAggAccount.setDouble(16, rsSelAccount.getDouble(18));
                        pstmtUpdateAggAccount.setDouble(17, rsSelAccount.getDouble(19));
                        pstmtUpdateAggAccount.setDouble(18, rsSelAccount.getDouble(20));
                        pstmtUpdateAggAccount.setLong(19, rsSelAccount.getLong(21));
                        pstmtUpdateAggAccount.setDate(20, rsSelAccount.getDate(22));
                        pstmtUpdateAggAccount.setDate(21, rsSelAccount.getDate(23));
                        // valores debitos limites vencido
                        pstmtUpdateAggAccount.setDouble(22, rsSelAccount.getDouble(24));
                        pstmtUpdateAggAccount.setDouble(23, rsSelAccount.getDouble(25));
                        // valores debitos limites vencer
                        pstmtUpdateAggAccount.setDouble(24, rsSelAccount.getDouble(26));
                        pstmtUpdateAggAccount.setDouble(25, rsSelAccount.getDouble(27));
                        // valores debitos limites totais
                        pstmtUpdateAggAccount.setDouble(26, rsSelAccount.getDouble(28));
                        pstmtUpdateAggAccount.setDouble(27, rsSelAccount.getDouble(29));
                        // dias de mora
                        pstmtUpdateAggAccount.setLong(28, rsSelAccount.getLong(30));
                        pstmtUpdateAggAccount.setLong(29, rsSelAccount.getLong(31));
                        // where customer, account
                        pstmtUpdateAggAccount.setLong(30, rsSelAccount.getLong(1));
                        pstmtUpdateAggAccount.setLong(31, rsSelAccount.getLong(2));

                        pstmtUpdateAggAccount.addBatch();

                    }

                }

            }

            pstmtUpdateAggAccount.executeBatch();

        }catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    static public void customerEventHandler(Connection conn) throws Exception {
        ResultSet rsSelCustomer;
        long customerId;

        pstmtAggCustomer = conn.prepareStatement(sqlQueryAggCustomer);
        pstmtUpdateAggCustomer = conn.prepareStatement(sqlUpdateAggCustomer);
        Iterator itCustomer = customerAccountHashMap.keySet().iterator();

        try {

            customerAccountHashMap.entrySet().forEach(entry -> {
                System.out.println("Customer Id: " + entry.getKey());
            });

            while (itCustomer.hasNext()) {
                customerId = (Long) itCustomer.next();

                pstmtAggCustomer.setLong(1, customerId);
                rsSelCustomer = pstmtAggCustomer.executeQuery();

                while (rsSelCustomer.next()) {
                    // valores vencido
                    pstmtUpdateAggCustomer.setDouble(1, rsSelCustomer.getDouble(2));
                    pstmtUpdateAggCustomer.setDouble(2, rsSelCustomer.getDouble(3));
                    pstmtUpdateAggCustomer.setDouble(3, rsSelCustomer.getDouble(4));
                    pstmtUpdateAggCustomer.setDouble(4, rsSelCustomer.getDouble(5));
                    pstmtUpdateAggCustomer.setLong(5, rsSelCustomer.getLong(6));
                    pstmtUpdateAggCustomer.setDate(6, rsSelCustomer.getDate(7));
                    pstmtUpdateAggCustomer.setDate(7, rsSelCustomer.getDate(8));
                    // valores vencer
                    pstmtUpdateAggCustomer.setDouble(8, rsSelCustomer.getDouble(9));
                    pstmtUpdateAggCustomer.setDouble(9, rsSelCustomer.getDouble(10));
                    pstmtUpdateAggCustomer.setDouble(10, rsSelCustomer.getDouble(11));
                    pstmtUpdateAggCustomer.setDouble(11, rsSelCustomer.getDouble(12));
                    pstmtUpdateAggCustomer.setLong(12, rsSelCustomer.getLong(13));
                    pstmtUpdateAggCustomer.setDate(13, rsSelCustomer.getDate(14));
                    pstmtUpdateAggCustomer.setDate(14, rsSelCustomer.getDate(15));
                    // valores totais
                    pstmtUpdateAggCustomer.setDouble(15, rsSelCustomer.getDouble(16));
                    pstmtUpdateAggCustomer.setDouble(16, rsSelCustomer.getDouble(17));
                    pstmtUpdateAggCustomer.setDouble(17, rsSelCustomer.getDouble(18));
                    pstmtUpdateAggCustomer.setDouble(18, rsSelCustomer.getDouble(19));
                    pstmtUpdateAggCustomer.setLong(19, rsSelCustomer.getLong(20));
                    pstmtUpdateAggCustomer.setDate(20, rsSelCustomer.getDate(21));
                    pstmtUpdateAggCustomer.setDate(21, rsSelCustomer.getDate(22));
                    // valores debitos limites vencido
                    pstmtUpdateAggCustomer.setDouble(22, rsSelCustomer.getDouble(23));
                    pstmtUpdateAggCustomer.setDouble(23, rsSelCustomer.getDouble(24));
                    // valores debitos limites vencer
                    pstmtUpdateAggCustomer.setDouble(24, rsSelCustomer.getDouble(25));
                    pstmtUpdateAggCustomer.setDouble(25, rsSelCustomer.getDouble(26));
                    // valores debitos limites totais
                    pstmtUpdateAggCustomer.setDouble(26, rsSelCustomer.getDouble(27));
                    pstmtUpdateAggCustomer.setDouble(27, rsSelCustomer.getDouble(28));
                    // dias de mora
                    pstmtUpdateAggCustomer.setLong(28, rsSelCustomer.getLong(29));
                    pstmtUpdateAggCustomer.setLong(29, rsSelCustomer.getLong(30));
                    // where customer, account
                    pstmtUpdateAggCustomer.setLong(30, rsSelCustomer.getLong(1));

                    pstmtUpdateAggCustomer.addBatch();

                }

            }

            pstmtUpdateAggCustomer.executeBatch();

        }catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }


    static public void printRow(ResultSet rs) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        int colsCount = rsmd.getColumnCount();
        for (int i = 1; i <= colsCount ; i++) {
            System.out.print(rsmd.getColumnName(i) + ": " +  rs.getString(i) + " " );
        }
        System.out.println("");
    }

}
