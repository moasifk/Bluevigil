create table FileHashLogData(LOG_TIMESTAMP VARCHAR NOT NULL,LOG_DATE DATE,LOG_TIME VARCHAR,VERSION VARCHAR,CLIENT VARCHAR,HOST VARCHAR,ANALYZERS VARCHAR,CONN_UIDS VARCHAR,DEPTH INTEGER,DURATION DOUBLE,FUID VARCHAR NOT NULL,IS_ORIG BOOLEAN,LOCAL_ORIG BOOLEAN,MD5 VARCHAR,MIME_TYPE VARCHAR,MISSING_BYTES INTEGER,OVERFLOW_BYTES INTEGER,RX_HOSTS VARCHAR NOT NULL,SEEN_BYTES INTEGER,SHA1 VARCHAR,SOURCE VARCHAR,TIMEDOUT BOOLEAN,TX_HOSTS VARCHAR,LABEL VARCHAR,INSERT_DATE_TIME VARCHAR,FILE_HANDLE VARCHAR CONSTRAINT pk PRIMARY KEY (LOG_TIMESTAMP,FUID,RX_HOSTS));

CREATE TABLE ConnectionLogData(UID VARCHAR NOT NULL,DEST_IP VARCHAR NOT NULL,DEST_LOCATION VARCHAR,DEST_PORT INTEGER NOT NULL,SOURCE_IP VARCHAR,SOURCE_LOCATION VARCHAR,SOURCE_PORT INTEGER,LOG_TIMESTAMP VARCHAR NOT NULL,LOG_DATE DATE,LOG_TIME VARCHAR,CLIENT VARCHAR,HOST VARCHAR,VERSION VARCHAR,CONN_STATE VARCHAR,DURATION DOUBLE,HISTORY VARCHAR,LOCAL_ORIG BOOLEAN,LOCAL_RESP BOOLEAN,MISSED_BYTES INTEGER,ORIG_BYTES INTEGER,ORIG_PKTS INTEGER ,ORIG_IP_BYTES INTEGER,RESP_IP_BYTES INTEGER,PROTO VARCHAR,RESP_BYTES INTEGER,RESP_PKTS INTEGER,TUNNEL_PARENTS VARCHAR,SERVICE VARCHAR,INSERT_DATE_TIME VARCHAR,FILE_HANDLE VARCHAR CONSTRAINT pk PRIMARY KEY (UID,DEST_IP,DEST_PORT,LOG_TIMESTAMP));

create table HttpLogData(LOG_TIMESTAMP VARCHAR NOT NULL,LOG_DATE DATE,LOG_TIME VARCHAR,VERSION VARCHAR,CLIENT VARCHAR,HOST VARCHAR,DEST_IP VARCHAR NOT NULL,DEST_COUNTRY VARCHAR,DEST_CITY VARCHAR,DEST_PORT INTEGER NOT NULL,SOURCE_IP VARCHAR,SOURCE_COUNTRY VARCHAR,SOURCE_CITY VARCHAR,SOURCE_PORT INTEGER,UID VARCHAR NOT NULL,METHOD VARCHAR,PROXIED VARCHAR,REFERER VARCHAR,REQUEST_BODY_LEN INTEGER,DEST_FUIDS VARCHAR,DEST_MIME_TYPES VARCHAR,RESPONSE_BODY_LEN INTEGER,STATUS_CODE INTEGER,STATUS_MSG VARCHAR,TAGS VARCHAR,TRANS_DEPTH INTEGER,URI VARCHAR,USER_AGENT VARCHAR,DEST_FILENAMES VARCHAR,SOURCE_MIME_TYPES VARCHAR,INFO_MSG VARCHAR,USERNAME VARCHAR,SOURCE_FUIDS VARCHAR,INFO_CODE INTEGER,SOURCE_FILENAMES VARCHAR,LABEL VARCHAR,INSERT_DATE_TIME VARCHAR,FILE_HANDLE VARCHAR CONSTRAINT pk PRIMARY KEY(LOG_TIMESTAMP,DEST_IP,DEST_PORT,UID));

create table DnsLogData(LOG_TIMESTAMP VARCHAR NOT NULL,LOG_DATE DATE,LOG_TIME VARCHAR,VERSION VARCHAR,CLIENT VARCHAR,DEST_IP VARCHAR NOT NULL,DEST_COUNTRY VARCHAR,DEST_CITY VARCHAR,DEST_PORT INTEGER NOT NULL,SOURCE_IP VARCHAR,SOURCE_COUNTRY VARCHAR,SOURCE_CITY VARCHAR,SOURCE_PORT INTEGER,UID VARCHAR NOT NULL,QCLASS INTEGER,QTYPE_NAME VARCHAR,QUERY VARCHAR,QCLASS_NAME VARCHAR,PROTOCOL  VARCHAR,QTYPE INTEGER,RD BOOLEAN,RA BOOLEAN,RCODE INTEGER,TC BOOLEAN,TRANS_ID INTEGER,AA BOOLEAN,REJECTED BOOLEAN,ANSWERS VARCHAR,Z INTEGER,RTT DOUBLE,TTLS VARCHAR,RCODE_NAME VARCHAR,LABEL VARCHAR,PATH VARCHAR,INSERT_DATE_TIME VARCHAR,FILE_HANDLE VARCHAR CONSTRAINT pk PRIMARY KEY(LOG_TIMESTAMP,DEST_IP,DEST_PORT,UID));
