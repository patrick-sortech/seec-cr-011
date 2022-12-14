
/**
 ** Please don't modify this file.
 ** This file was generated by the EntireX Java Wrapper
 ** from the IDL file SITAFDES.idl.
 **
 ** @author    Software AG
 ** @version   EntireX Wrapper, Version: 10.7.0.0.1262, Date: 17 Feb 2021
 ** Generated: 2022-08-10 20:05:31.719 -0300
 **/

import java.lang.reflect.Method;

import com.softwareag.entirex.aci.*;

public class AtualizarDebitos extends RPCService {

    public static final String DEFAULT_BROKERID = "etb010";
    public static final String DEFAULT_SERVER = "RPC/MAQH171X/CALLNAT";

    private final static String N40017OX_FORMAT = // from IDL
            ("OA8,0OA41,0.");

    public AtualizarDebitos() {
        super();
    }

    public AtualizarDebitos(Broker broker) {
        this(broker, DEFAULT_SERVER);
    }

    public AtualizarDebitos(BrokerService bs) {
        this(bs.getBroker(), bs.toString());
    }

    public AtualizarDebitos(Broker broker, String serverAddr) {
        super(broker, serverAddr, "SITAFDES", true);
    }

    public static String getStubVersion() {
        return "EntireX RPC for Java Stub Version=10.7.0.0, Patch Level=1262";
    }
    public void n40017ox(String _p_e_dt_correcao, String _p_e_chave_pesquisa) throws BrokerException {


        enterStub();
        buildHeader(N40017OX_FORMAT, "2", 0, 0, "N40017OX", true, 1110, false, false, 2050);
        if (getVersion() >= 2000) {

            if (_p_e_dt_correcao == null) {
                super.marshal.addDataA(null, 8);
            } else {
                super.marshal.addDataA(_p_e_dt_correcao, 8);
            }

            if (_p_e_chave_pesquisa == null) {
                super.marshal.addDataA(null, 41);
            } else {
                super.marshal.addDataA(_p_e_chave_pesquisa, 41);
            }
        } else {
            super.marshal.addDataA(_p_e_dt_correcao, 8);
            super.marshal.addDataA(_p_e_chave_pesquisa, 41);
        }

        callServer();

        if (getVersion() >= 2000) {
        } else {
            if (!super.compression) {
                super.marshal.skipData(8);
            }
            if (!super.compression) {
                super.marshal.skipData(41);
            }
        }
        leaveStub();

    }

    public static final Method enterStubMethod = getStubMethod("enterClientStub");
    public static final Method leaveStubMethod = getStubMethod("leaveClientStub");

    private static Method getStubMethod(String name) {
        Method method = null;
        try {
            method = RPCService.class.getMethod(name, (Class<?>[]) null);
        } catch (final Exception ex) {
        }
        return method;
    }

    private void enterStub() {
        if (enterStubMethod != null) {
            try {
                enterStubMethod.invoke(this, (Object[]) null);
            } catch (final Exception ex) {
            }
        }
    }

    private void leaveStub() {
        if (leaveStubMethod != null) {
            try {
                leaveStubMethod.invoke(this, (Object[]) null);
            } catch (final Exception ex) {
            }
        }
    }
}
