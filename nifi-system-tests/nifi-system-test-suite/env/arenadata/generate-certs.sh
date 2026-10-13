#!/bin/bash

#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# Generate SSL certificates for NiFi cluster nodes

set -e

CERT_DIR="./nifi-certs"
DAYS_VALID=365

# Create directory for certificates
mkdir -p "$CERT_DIR"

echo "Generating CA certificate..."
# Generate CA private key
openssl genrsa -out "$CERT_DIR/ca-key.pem" 2048

# Generate CA certificate
openssl req -x509 -new -nodes \
  -key "$CERT_DIR/ca-key.pem" \
  -sha256 \
  -days $DAYS_VALID \
  -out "$CERT_DIR/ca-cert.pem" \
  -subj "/C=US/ST=CA/L=Santa Monica/O=NiFi Test/CN=NiFi Test CA"

echo "CA certificate generated"

# Function to generate certificate for a node
generate_node_cert() {
  local node_name=$1
  local node_hostname=$2

  echo "Generating certificate for $node_name ($node_hostname)..."

  # Generate private key
  openssl genrsa -out "$CERT_DIR/${node_name}-key.pem" 2048

  # Generate certificate signing request
  openssl req -new \
    -key "$CERT_DIR/${node_name}-key.pem" \
    -out "$CERT_DIR/${node_name}.csr" \
    -subj "/C=US/ST=CA/L=Santa Monica/O=NiFi Test/CN=${node_hostname}"

  # Create config file for SAN
  cat > "$CERT_DIR/${node_name}-san.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${node_hostname}
DNS.2 = ${node_name}
DNS.3 = localhost
IP.1 = 127.0.0.1
EOF

  # Sign certificate with CA
  openssl x509 -req \
    -in "$CERT_DIR/${node_name}.csr" \
    -CA "$CERT_DIR/ca-cert.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -out "$CERT_DIR/${node_name}-cert.pem" \
    -days $DAYS_VALID \
    -sha256 \
    -extfile "$CERT_DIR/${node_name}-san.cnf" \
    -extensions v3_req

  # Create PKCS12 keystore
  openssl pkcs12 -export \
    -in "$CERT_DIR/${node_name}-cert.pem" \
    -inkey "$CERT_DIR/${node_name}-key.pem" \
    -out "$CERT_DIR/${node_name}-keystore.p12" \
    -name "$node_name" \
    -password pass:changeit
  # Build truststore as Java trustedCertEntry (required for PKIX trust anchors)
  # NOTE: openssl pkcs12 -export -nokeys may produce entries that are not
  # interpreted by Java as trustedCertEntry, causing:
  # InvalidAlgorithmParameterException: the trustAnchors parameter must be non-empty
  if [ ! -f "$CERT_DIR/truststore.p12" ]; then
    keytool -importcert \
      -keystore "$CERT_DIR/truststore.p12" \
      -storetype PKCS12 \
      -storepass changeit \
      -noprompt \
      -alias nifi-ca \
      -file "$CERT_DIR/ca-cert.pem"
    echo "Generated truststore.p12 with trustedCertEntry alias nifi-ca"
  fi

  echo "Certificate for $node_name generated"
}

# Generate certificates for all NiFi nodes
generate_node_cert "nifi01" "nifi01.demo"
generate_node_cert "nifi02" "nifi02.demo"
generate_node_cert "nifi03" "nifi03.demo"

echo ""
echo "All certificates generated successfully in $CERT_DIR/"
echo "Keystore password: changeit"
echo "Truststore password: changeit"
