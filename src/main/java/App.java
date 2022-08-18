import br.com.sortech.utils.configurations.database.ConnectionManager;
import com.softwareag.entirex.aci.*;
import oracle.jdbc.OraclePreparedStatement;

import javax.xml.transform.Result;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class App {

    public static int BATCH_SIZE = 5000;
    public static int ROWS_LIMIT = 155000;

    public static int ROWS_OFFSET = 10000;


    public static void main(String[] args) throws Exception {
        Connection connOracle = null;
        Connection connAdabas = null;

        connOracle = getConexaoOracle();
        connOracle.setAutoCommit(false);

        connAdabas = getConexaoAdabas();
        connAdabas.setAutoCommit(false);

        com.softwareag.entirex.aci.Broker broker = null;
        AtualizarDebitos a = null;

        String chaveDebitos = "20220818LOTE0001";

        broker = new com.softwareag.entirex.aci.Broker("10.69.1.14:1971", "RPC/MAQH171X/CALLNAT");
        a = new AtualizarDebitos(broker,"RPC/MAQH171X/CALLNAT");
//        a.setRPCUserId("03249595");
//        a.setRPCPassword("nova01");
//        a.setRPCUserId("_UsuAdmGwhomolog");
//        a.setRPCPassword("#GW_homolog$1");
//        a.setNaturalLogon(true);

        broker.logon();
        a.setReliable(1); // Seguramente deixou a chamada assíncrona


        try{
            ResultSet rsOracle = getDebitosOracle(connOracle);
            carregaDebitosAdabas(connAdabas, rsOracle, chaveDebitos);
//
            System.out.println("* ------ CHAMADA ENTIREX AO N40017OX ------*");

            a.n40017ox("", chaveDebitos);
            System.out.println("* ------ VERIFICANDO TERMINO EXECUCAO ------*");
            long startTime = System.nanoTime();
            boolean finalizado = false;
            do {
                TimeUnit.SECONDS.sleep(10);
                finalizado = verificaTerminoExecucao(connAdabas, chaveDebitos);

            }while(finalizado != true);
            long endTime = System.nanoTime();
            long totalTime =  TimeUnit.SECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Tempo de Execução: " + totalTime + " segundos");
            System.out.println("");

            ResultSet rsAdabas = getDebitosCorrigidosAdabas(connAdabas, chaveDebitos);
            atualizaDebitosOracle(connOracle, rsAdabas);
        }catch(SQLException e){
            connOracle.rollback();
            connOracle.close();
            connOracle = null;
            connAdabas.rollback();
            connAdabas.close();
            connAdabas = null;
        }catch(Exception e){
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }finally{
            if (connOracle != null) {
                connOracle.commit();
                connOracle.close();
                connOracle = null;
            }
            if (connAdabas != null) {
                connAdabas.commit();
                connAdabas.close();
                connAdabas = null;
            }
        }

    }

    static public ResultSet getDebitosOracle(Connection connection) throws SQLException {
        System.out.println("* ------ DEBITOS RECUPERADOS DO ORACLE ------*");
        ResultSet rs = null;
        String sqlGetDebitos = "SELECT \n" +
                "    NU_DEVEDOR_RESPONSAVEL,\n" +
                "    ID_DEBITO_EXT,\n" +
                "    CO_RECEITA_PRINCIPAL,\n" +
                "    CO_TRIBUTO_EXT,\n" +
                "    NU_COTA,\n" +
                "    NU_GUIA_LANCAMENTO,\n" +
                "    DT_ANO_LANCAMENTO,\n" +
                "    NU_PARCELA_LANCAMENTO,\n" +
                "    ST_PAGAMENTO,\n" +
                "    ST_LANCAMENTO,\n" +
                "    EXT_BILL_ID,\n" +
                "    ST_CANCELAMENTO\n" +
                "FROM ICS_IT_X_BILL\n" +
                "WHERE ST_PAGAMENTO = '00'\n" +
                "AND CO_TRIBUTO_EXT = 1\n" +
                "OFFSET " + ROWS_OFFSET + " ROWS\n" +
                "";
        try {
            long startTime = System.nanoTime();

            PreparedStatement ps = connection.prepareStatement(sqlGetDebitos);
            ps.setMaxRows(ROWS_LIMIT);
            rs = ps.executeQuery();

            long endTime = System.nanoTime();
            long totalTime =  TimeUnit.SECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Tempo de Execução: " + totalTime + " segundos");
            System.out.println("");


        } catch (SQLException ex) {
//            connection.rollback();
//            connection.close();
            ex.printStackTrace();
        }
        return rs;
    }

    static public void printRow(ResultSet rs) throws Exception {
        if (rs != null) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colsCount = rsmd.getColumnCount();
            for (int i = 1; i <= colsCount; i++) {
                System.out.print(rsmd.getColumnName(i) + ": " + rs.getString(i) + " ");
            }
            System.out.println("");
        }
    }

    static public Connection getConexaoOracle() throws Exception {
        Properties props = new Properties();
        props.load(App.class.getClassLoader().getResourceAsStream("database.properties"));
        ConnectionManager connMgr = new ConnectionManager(props.getProperty("datasource.driver-class-name")
                , props.getProperty("datasource.url"), props.getProperty("datasource.username"), props.getProperty("datasource.password"));

        return connMgr.getConnection();
    }

    static public Connection getConexaoAdabas() throws Exception {
        String url = "jdbc:connx:DD=PATRICK;Gateway=aplsrvhml003;PORT=7500";
        String user = "_UsuAdmGwhomolog";
        String password = "#GW_homolog$1";

        Class.forName("com.Connx.jdbc.TCJdbc.TCJdbcDriver").newInstance();

        return DriverManager.getConnection(url, user, password);
    }

    static public ResultSet getDebitosCorrigidosAdabas(Connection connection, String chaveDebitos) throws SQLException {
        System.out.println("* ------ DEBITOS CORRIGIDOS RECUPERADOS DO ADABAS ------*");
        ResultSet result = null;
        String query = "SELECT \r\n" +
                "       IT_CHAVE_ORIGEM_DEBITO,\r\n" +
                "       IT_VA_PRINCIPAL,\r\n" +
                "       IT_VA_MULTA,\r\n" +
                "       IT_VA_JUROS,\r\n" +
                "       IT_VA_OUTROS,\r\n" +
                "       IT_VA_TOTAL,\r\n" +
                "       IT_IN_SIT_DEBITO,\r\n" +
                "       IT_DA_CORRECAO,\r\n" +
                "       IT_MSG_ERRO_DEBITO,\r\n" +
                "       IT_CO_ERRO\r\n" +
                "FROM SITAF_DEBITOS_CORRECAO\r\n" +
                "WHERE IT_CHAVE_PESQUISA = '" + chaveDebitos + "'\r\n" +
                "AND IT_CO_ERRO IS NULL";

        try {
            long startTime = System.nanoTime();

            Statement st = connection.createStatement();
            result = st.executeQuery(query);

            long endTime = System.nanoTime();
            long totalTime =  TimeUnit.SECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Tempo de Execução: " + totalTime + " segundos");
            System.out.println("");

        } catch (SQLException ex) {
//            connection.rollback();
//            connection.close();
            ex.printStackTrace();
        }
//        } finally {
//            if (connection != null) {
//                connection.close();
//                // connection = null;
//            }
//        }

        return result;
    }

    static public void carregaDebitosAdabas(
            Connection connection,
            ResultSet rsDebitosOracle,
            String chaveDebitos
    ) throws SQLException {
        System.out.println("* ------ CARREGAR DEBITOS NO ADABAS ------*");

        int countLote = 0;
        int countTotal = 0;
        int[] qtdBatch;

        String sqlInsert = "INSERT INTO SITAF_DEBITOS_CORRECAO ( IT_NU_TERMINAL, IT_CO_USUARIO," +
                "IT_IDEN_DEBITO,IT_IN_TIPO_DEBITO,IT_NU_RECEITA,IT_NU_ANO_REF_DEBITO,IT_CHAVE_PESQUISA, " +
                "IT_NU_COTA_DEBITO, IT_CHAVE_ORIGEM_DEBITO) VALUES (?,?,?,?,?,?,?,?,?)";
        try {
            long startTime = System.nanoTime();

            PreparedStatement ps = connection.prepareStatement(sqlInsert);

            while (rsDebitosOracle.next()) {
                //printRow(rsDebitosOracle);
                ps.setString(1, "SICOB");
                ps.setString(2, "INTERNET");
                ps.setString(3, rsDebitosOracle.getString("ID_DEBITO_EXT"));
                switch(rsDebitosOracle.getInt("CO_TRIBUTO_EXT")){
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 7:
                        ps.setString(4, "L");
                        break;
                    case 9:
                        ps.setString(3, rsDebitosOracle.getString("NU_DEVEDOR_RESPONSAVEL"));
                        ps.setString(4, "L");
                        break;
                    case 5:
                    case 6:
                        ps.setString(4, "D");
                        break;
                    case 8:
                        ps.setString(4, "P");
                        break;
                }

                ps.setInt(5, rsDebitosOracle.getInt("CO_RECEITA_PRINCIPAL"));
                ps.setInt(6, rsDebitosOracle.getInt("DT_ANO_LANCAMENTO"));
                ps.setString(7, chaveDebitos);
                ps.setString(8, rsDebitosOracle.getString("NU_PARCELA_LANCAMENTO"));
                ps.setString(9, rsDebitosOracle.getString("EXT_BILL_ID"));

                ps.addBatch();
                countLote++;
                countTotal++;

                if(countLote >= BATCH_SIZE){
                    qtdBatch = ps.executeBatch();
                    ps.clearBatch();
//                    System.out.println("Executa Batch ..: " + Arrays.toString(qtdBatch));
                    connection.commit();
                    countLote = 0;
                }
            }
            System.out.println("TOTAL DE DEBITOS CARREGADOS ADABAS => " + countTotal);
            qtdBatch = ps.executeBatch();
//            System.out.println("Último Executa Batch ..: " + Arrays.toString(qtdBatch));
            connection.commit();

            long endTime = System.nanoTime();
            long totalTime =  TimeUnit.SECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Tempo de Execução: " + totalTime + " segundos");
            System.out.println("");

        } catch (Exception ex) {
            System.out.println("ERRO TOTAL DE DEBITOS CARREGADOS ADABAS => " + countTotal);

//            connection.rollback();
//            connection.close();
            ex.printStackTrace();
        }
//        } finally {
//            System.out.println("Finally => " + connection);
//            if (connection != null) {
//                connection.commit();
//                //connection.close();
//                //connection = null;
//            }
//
//        }

    }

    static public void atualizaDebitosOracle(
            Connection connection,
            ResultSet rsDebitosAdabas
    ) throws SQLException {
        System.out.println("* ------ CARREGAR DEBITOS CORRIGIDOS NO ORACLE ------*");

        int countLote = 0;
        int countTotal = 0;

        String sqlUpdate = "UPDATE ICS_IT_X_BILL\n" +
                "SET\n" +
                "VL_ATU_PRINCIPAL = ?,\n" +
                "VL_ATU_MULTAS = ?,\n" +
                "VL_ATU_JUROS = ?,\n" +
                "VL_ATU_OUTROS = ?,\n" +
                "VL_DIVIDA_ATUAL = ?,\n" +
                "DT_ULT_ATU_DIVIDA = TO_DATE(?,'YYYYMMDD')\n" +
                "WHERE\n" +
                "EXT_BILL_ID = ?\n";

        try{
            long startTime = System.nanoTime();

            PreparedStatement ps = connection.prepareStatement(sqlUpdate);
            while (rsDebitosAdabas.next()) {

                    //printRow(rsDebitosAdabas);
                    ps.setDouble(1, rsDebitosAdabas.getDouble("IT_VA_PRINCIPAL"));
                    ps.setDouble(2, rsDebitosAdabas.getDouble("IT_VA_MULTA"));
                    ps.setDouble(3, rsDebitosAdabas.getDouble("IT_VA_JUROS"));
                    ps.setDouble(4, rsDebitosAdabas.getDouble("IT_VA_OUTROS"));
                    ps.setDouble(5, rsDebitosAdabas.getDouble("IT_VA_TOTAL"));
                    ps.setString(6, rsDebitosAdabas.getString("IT_DA_CORRECAO"));
                    ps.setString(7, rsDebitosAdabas.getString("IT_CHAVE_ORIGEM_DEBITO").trim());

                    ps.addBatch();
                    countLote++;
                    countTotal++;

                    if (countLote >= BATCH_SIZE) {
                        ps.executeBatch();
                        connection.commit();
                        ps.clearBatch();
                        countLote = 0;
                    }
                }

            System.out.println("TOTAL ATUALIZADOS ORACLE => " + countTotal);
            int[] qtdBatch = ps.executeBatch();
            //System.out.println("Último Executa Batch ..: " + Arrays.toString(qtdBatch));
            connection.commit();

            long endTime = System.nanoTime();
            long totalTime =  TimeUnit.SECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Tempo de Execução: " + totalTime + " segundos");
            System.out.println("");
        } catch (SQLException ex) {
            System.out.println("ERRO TOTAL LIDOS PARA UPDATE => " + countTotal);
//            connection.rollback();
//            connection.close();
            ex.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
//        } finally {
//            System.out.println("Finally => " + connection);
//            if (connection != null) {
//                connection.commit();
//                connection.close();
//                //connection = null;
//            }
//
//        }
    }

    public static boolean verificaTerminoExecucao(Connection connection, String chaveDebito){
        System.out.println("Verificar se finalizou execucao ..: " + chaveDebito);
        String query = "SELECT IT_IN_SITUACAO_PROCESSAMENTO FROM SITAF_CONTROLE_EXECUCAO WHERE GR_IDEN_DEBITO = '" + chaveDebito +"'";
        boolean execucaoFinalizada = false;
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            if(rs != null){
                String res = rs.getString("IT_IN_SITUACAO_PROCESSAMENTO");
                System.out.println("IT_IN_SITUACAO_PROCESSAMENTO = " + res);
                execucaoFinalizada = (res.equals("S")) ? true : false;
            }
        }catch (Exception e ){
            e.printStackTrace();
            execucaoFinalizada = false;
        }
        return execucaoFinalizada;
    }

    public static void setTimeoutSync(Runnable runnable, int delay) {
        try {
            Thread.sleep(delay);
            runnable.run();
        }
        catch (Exception e){
            System.err.println(e);
        }
    }

}