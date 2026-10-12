#!/bin/bash


echo "==== Add appuser ========="
adduser appuser
echo ""

echo "==== Kerberos KDC and Kadmin ========"
KADMIN_PRINCIPAL_FULL=$KADMIN_PRINCIPAL@$REALM
echo "REALM: $REALM"
echo "KADMIN_PRINCIPAL_FULL: $KADMIN_PRINCIPAL_FULL"
echo "KADMIN_PASSWORD: $KADMIN_PASSWORD"
echo ""

echo "==== Creating realm ========"
/usr/sbin/kdb5_util create -s -r $REALM -P $MASTER_PASSWORD
echo ""

echo "==== Creating default principals in the acl ==========="
echo "Adding $KADMIN_PRINCIPAL principal"
kadmin.local -q "delete_principal -force $KADMIN_PRINCIPAL_FULL"
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD $KADMIN_PRINCIPAL_FULL"
echo ""

echo "==== Creating NiFi principals =========="
kadmin.local -q "add_principal -randkey nifi/nifi01.demo@DEMO"
kadmin.local -q "add_principal -randkey nifi/nifi02.demo@DEMO"
kadmin.local -q "add_principal -randkey nifi/nifi03.demo@DEMO"
kadmin.local -q "add_principal -randkey HTTP/nifi01.demo@DEMO"
kadmin.local -q "add_principal -randkey HTTP/nifi02.demo@DEMO"
kadmin.local -q "add_principal -randkey HTTP/nifi03.demo@DEMO"
kadmin.local -q "add_principal -pw test-pass1 test-user1@DEMO"
kadmin.local -q "add_principal -pw test-pass2 test-user2@DEMO"
kadmin.local -q "add_principal -pw admin1234567890 admin@DEMO"
echo ""

echo "==== Remove old keytabs  =========="
rm -rf /tmp/secrets/*.keytab
echo ""

echo "==== Export keytabs for NiFi =========="
kadmin.local -q "xst -kt /tmp/secrets/nifi01.keytab nifi/nifi01.demo@DEMO HTTP/nifi01.demo@DEMO" && chown appuser:appuser /tmp/secrets/nifi01.keytab
kadmin.local -q "xst -kt /tmp/secrets/nifi02.keytab nifi/nifi02.demo@DEMO HTTP/nifi02.demo@DEMO" && chown appuser:appuser /tmp/secrets/nifi02.keytab
kadmin.local -q "xst -kt /tmp/secrets/nifi03.keytab nifi/nifi03.demo@DEMO HTTP/nifi03.demo@DEMO" && chown appuser:appuser /tmp/secrets/nifi03.keytab
echo ""

krb5kdc
kadmind -nofork