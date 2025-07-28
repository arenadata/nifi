/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
