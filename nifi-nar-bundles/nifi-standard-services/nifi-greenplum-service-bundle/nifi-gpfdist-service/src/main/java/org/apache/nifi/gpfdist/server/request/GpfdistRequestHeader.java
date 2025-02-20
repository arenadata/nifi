package org.apache.nifi.gpfdist.server.request;

public class GpfdistRequestHeader {
    public static final String X_GP_XID = "X-GP-XID";
    public static final String X_GP_CID = "X-GP-CID";
    public static final String X_GP_SN = "X-GP-SN";
    public static final String X_GP_SEGMENT_ID = "X-GP-SEGMENT-ID";
    public static final String X_GP_SEGMENT_COUNT = "X-GP-SEGMENT-COUNT";
    public static final String X_GP_LINE_DELIM_LENGTH = "X-GP-LINE-DELIM-LENGTH";
    public static final String X_GP_PROTO = "X-GP-PROTO";
    public static final String X_GP_MASTER_HOST = "X-GP-MASTER_HOST";
    public static final String X_GP_MASTER_PORT = "X-GP-MASTER_PORT";
    public static final String X_GP_CSV_OPT = "X-GP-CSVOPT";
    public static final String X_GP_SEG_PG_CONF = "X-GP_SEG_PG_CONF";
    public static final String X_GP_SEG_DATADIR = "X-GP_SEG_DATADIR";
    public static final String X_GP_DATABASE = "X-GP-DATABASE";
    public static final String X_GP_X_GP_USER = "X-GP-X-GP-USER";
    public static final String X_GP_X_SEG_PORT = "X-GP-X-GP-SEG-PORT";
    public static final String X_GP_SESSION_ID = "x-gp-session-id";

    private GpfdistRequestHeader() {
    }
}
