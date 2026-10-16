#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

RANGER_URL="${RANGER_URL:-http://ranger-admin:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASSWORD="${RANGER_PASSWORD:-rangerR0cks!}"
NIFI_SERVICE_NAME="${NIFI_SERVICE_NAME:-nifi}"
NIFI_SERVICE_TYPE="${NIFI_SERVICE_TYPE:-nifi}"
BOOTSTRAP_POLICY_NAME="${BOOTSTRAP_POLICY_NAME:-nifi-admin-bootstrap}"
PROXY_BOOTSTRAP_POLICY_NAME="${PROXY_BOOTSTRAP_POLICY_NAME:-nifi-proxy-bootstrap}"
AUTH="${RANGER_USER}:${RANGER_PASSWORD}"

cleanup() {
    rm -f "${POLICIES_FILE:-}" "${SERVICE_FILE:-}"
}

trap cleanup EXIT

curl_status() {
    curl -sS -o /dev/null -w "%{http_code}" -u "${AUTH}" "$1"
}

curl_get() {
    curl -sS -u "${AUTH}" "$1"
}

curl_post_json() {
    response_file="$(mktemp)"
    status="$(curl -sS -o "${response_file}" -w "%{http_code}" -u "${AUTH}" \
        -H "Content-Type: application/json" \
        -X POST \
        "$1" \
        --data "@$2")"

    cat "${response_file}"
    rm -f "${response_file}"

    case "${status}" in
        200|201|204)
            return 0
            ;;
        *)
            echo "ERROR: Ranger POST request to $1 failed with HTTP ${status}" >&2
            return 1
            ;;
    esac
}

ensure_ranger_user() {
    user_name="$1"
    user_file="$2"
    response_file="$(mktemp)"
    status="$(curl -sS -o "${response_file}" -w "%{http_code}" -u "${AUTH}" \
        -H "Content-Type: application/json" \
        -X POST \
        "${RANGER_URL}/service/xusers/users" \
        --data "@${user_file}")"

    cat "${response_file}"

    case "${status}" in
        200|201|204)
            rm -f "${response_file}"
            echo "Created Ranger user '${user_name}'"
            return 0
            ;;
        400)
            if grep -qi "already\|exist\|duplicate" "${response_file}"; then
                rm -f "${response_file}"
                echo "Ranger user '${user_name}' already exists"
                return 0
            fi
            ;;
    esac

    echo "ERROR: Ranger user creation failed for '${user_name}' with HTTP ${status}" >&2
    rm -f "${response_file}"
    return 1
}

ensure_ranger_group() {
    group_name="$1"
    group_file="$2"
    response_file="$(mktemp)"
    status="$(curl -sS -o "${response_file}" -w "%{http_code}" -u "${AUTH}" \
        -H "Content-Type: application/json" \
        -X POST \
        "${RANGER_URL}/service/xusers/groups" \
        --data "@${group_file}")"

    cat "${response_file}"

    case "${status}" in
        200|201|204)
            rm -f "${response_file}"
            echo "Created Ranger group '${group_name}'"
            return 0
            ;;
        400)
            if grep -qi "already\|exist\|duplicate" "${response_file}"; then
                rm -f "${response_file}"
                echo "Ranger group '${group_name}' already exists"
                return 0
            fi
            ;;
    esac

    echo "ERROR: Ranger group creation failed for '${group_name}' with HTTP ${status}" >&2
    rm -f "${response_file}"
    return 1
}

ensure_ranger_group_membership() {
    group_name="$1"
    membership_file="$2"
    response_file="$(mktemp)"
    status="$(curl -sS -o "${response_file}" -w "%{http_code}" -u "${AUTH}" \
        -H "Content-Type: application/json" \
        -X POST \
        "${RANGER_URL}/service/xusers/ugsync/groupusers" \
        --data "@${membership_file}")"

    cat "${response_file}"

    case "${status}" in
        200|201|204)
            rm -f "${response_file}"
            echo "Ensured Ranger group membership for '${group_name}'"
            return 0
            ;;
        400)
            if grep -qi "already\|exist\|duplicate" "${response_file}"; then
                rm -f "${response_file}"
                echo "Ranger group membership for '${group_name}' already exists"
                return 0
            fi
            ;;
    esac

    echo "ERROR: Ranger group membership update failed for '${group_name}' with HTTP ${status}" >&2
    rm -f "${response_file}"
    return 1
}

curl_put_json() {
    response_file="$(mktemp)"
    status="$(curl -sS -o "${response_file}" -w "%{http_code}" -u "${AUTH}" \
        -H "Content-Type: application/json" \
        -X PUT \
        "$1" \
        --data "@$2")"

    cat "${response_file}"
    rm -f "${response_file}"

    case "${status}" in
        200|201|204)
            return 0
            ;;
        *)
            echo "ERROR: Ranger PUT request to $1 failed with HTTP ${status}" >&2
            return 1
            ;;
    esac
}

echo "Waiting for Ranger Admin REST API at ${RANGER_URL}..."

for attempt in $(seq 1 60); do
    status="$(curl_status "${RANGER_URL}/service/public/v2/api/servicedef/name/${NIFI_SERVICE_TYPE}")"
    if [ "${status}" = "200" ]; then
        echo "Ranger Admin REST API is ready"
        break
    fi

    if [ "${attempt}" -eq 60 ]; then
        echo "ERROR: Ranger Admin REST API is not ready after ${attempt} attempts" >&2
        exit 1
    fi

    echo "Ranger Admin REST API is not ready yet, attempt ${attempt}/60"
    sleep 5
done

echo "Ensuring Ranger service definition '${NIFI_SERVICE_TYPE}' exists..."
service_def_status="$(curl_status "${RANGER_URL}/service/public/v2/api/servicedef/name/${NIFI_SERVICE_TYPE}")"
if [ "${service_def_status}" != "200" ]; then
    echo "ERROR: Ranger service definition '${NIFI_SERVICE_TYPE}' does not exist, HTTP ${service_def_status}" >&2
    exit 1
fi

echo "Enabling DENY policies for service definition '${NIFI_SERVICE_TYPE}'..."
SERVICE_DEF_FILE="$(mktemp)"
curl -sS -u "${AUTH}" "${RANGER_URL}/service/public/v2/api/servicedef/name/${NIFI_SERVICE_TYPE}" -o "${SERVICE_DEF_FILE}"
service_def_id="$(grep -o '"id"[[:space:]]*:[[:space:]]*[0-9][0-9]*' "${SERVICE_DEF_FILE}" | head -1 | grep -o '[0-9][0-9]*' || true)"
if [ -z "${service_def_id}" ]; then
    echo "ERROR: Failed to extract Ranger service definition id for '${NIFI_SERVICE_TYPE}'" >&2
    cat "${SERVICE_DEF_FILE}" >&2
    exit 1
fi

# Check if DENY policies are already enabled
if grep -q '"enableDenyAndExceptionsInPolicies"[[:space:]]*:[[:space:]]*"true"' "${SERVICE_DEF_FILE}"; then
    echo "DENY policies already enabled for '${NIFI_SERVICE_TYPE}'"
else
    # Update service definition to enable DENY policies
    sed -i 's/"enableDenyAndExceptionsInPolicies"[[:space:]]*:[[:space:]]*"false"/"enableDenyAndExceptionsInPolicies":"true"/' "${SERVICE_DEF_FILE}"
    curl_put_json "${RANGER_URL}/service/public/v2/api/servicedef/${service_def_id}" "${SERVICE_DEF_FILE}"
    echo "Enabled DENY policies for '${NIFI_SERVICE_TYPE}'"
fi
rm -f "${SERVICE_DEF_FILE}"

echo "Ensuring Ranger service '${NIFI_SERVICE_NAME}' exists..."
service_status="$(curl_status "${RANGER_URL}/service/public/v2/api/service/name/${NIFI_SERVICE_NAME}")"
if [ "${service_status}" = "200" ]; then
    echo "Ranger service '${NIFI_SERVICE_NAME}' already exists, updating configuration"
    SERVICE_FILE="$(mktemp)"
    curl -sS -u "${AUTH}" "${RANGER_URL}/service/public/v2/api/service/name/${NIFI_SERVICE_NAME}" -o "${SERVICE_FILE}.get"
    service_id="$(grep -o '"id"[[:space:]]*:[[:space:]]*[0-9][0-9]*' "${SERVICE_FILE}.get" | head -1 | grep -o '[0-9][0-9]*' || true)"
    if [ -z "${service_id}" ]; then
        echo "ERROR: Failed to extract Ranger service id for '${NIFI_SERVICE_NAME}'" >&2
        cat "${SERVICE_FILE}.get" >&2
        exit 1
    fi
    awk -v id="${service_id}" 'NR==1 { print; print "  \"id\": " id ","; next } { print }' /bootstrap/nifi-service.json > "${SERVICE_FILE}"
    curl_put_json "${RANGER_URL}/service/public/v2/api/service/${service_id}" "${SERVICE_FILE}"
    echo "Updated Ranger service '${NIFI_SERVICE_NAME}'"
elif [ "${service_status}" = "404" ]; then
    curl_post_json "${RANGER_URL}/service/public/v2/api/service" /bootstrap/nifi-service.json

    echo "Created Ranger service '${NIFI_SERVICE_NAME}'"
else
    echo "ERROR: Failed to check Ranger service '${NIFI_SERVICE_NAME}', HTTP ${service_status}" >&2
    curl_get "${RANGER_URL}/service/public/v2/api/service/name/${NIFI_SERVICE_NAME}" >&2 || true
    exit 1
fi

echo "Ensuring Ranger users for NiFi node certificate identities exist..."
ensure_ranger_user "CN=nifi01.demo, O=NiFi Test, L=Santa Monica, ST=CA, C=US" /bootstrap/nifi01-node-user.json
ensure_ranger_user "CN=nifi02.demo, O=NiFi Test, L=Santa Monica, ST=CA, C=US" /bootstrap/nifi02-node-user.json
ensure_ranger_user "CN=nifi03.demo, O=NiFi Test, L=Santa Monica, ST=CA, C=US" /bootstrap/nifi03-node-user.json

echo "Ensuring Ranger users and groups for NiFi authorization system tests exist..."
ensure_ranger_group "test-group1" /bootstrap/test-group1.json
ensure_ranger_user "test-user1" /bootstrap/test-user1.json
ensure_ranger_user "test-user2" /bootstrap/test-user2.json
ensure_ranger_group_membership "test-group1" /bootstrap/test-group1-membership.json

echo "Ensuring Ranger bootstrap policy '${BOOTSTRAP_POLICY_NAME}' exists..."
POLICIES_FILE="$(mktemp)"

policies_status="$(curl -sS -o "${POLICIES_FILE}" -w "%{http_code}" -u "${AUTH}" \
    "${RANGER_URL}/service/public/v2/api/policy?serviceName=${NIFI_SERVICE_NAME}")"

if [ "${policies_status}" != "200" ]; then
    echo "Policy lookup by serviceName returned HTTP ${policies_status}; trying service policy endpoint..."
    policies_status="$(curl -sS -o "${POLICIES_FILE}" -w "%{http_code}" -u "${AUTH}" \
        "${RANGER_URL}/service/public/v2/api/service/${NIFI_SERVICE_NAME}/policy")"
fi

if [ "${policies_status}" != "200" ]; then
    echo "ERROR: Failed to list Ranger policies for service '${NIFI_SERVICE_NAME}', HTTP ${policies_status}" >&2
    cat "${POLICIES_FILE}" >&2
    exit 1
fi

if grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${BOOTSTRAP_POLICY_NAME}\"" "${POLICIES_FILE}"; then
    echo "Ranger bootstrap policy '${BOOTSTRAP_POLICY_NAME}' already exists"
else
    curl_post_json "${RANGER_URL}/service/public/v2/api/policy" /bootstrap/nifi-admin-bootstrap-policy.json

    echo "Created Ranger bootstrap policy '${BOOTSTRAP_POLICY_NAME}'"
fi

echo "Ensuring Ranger bootstrap policy '${PROXY_BOOTSTRAP_POLICY_NAME}' exists..."
if grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${PROXY_BOOTSTRAP_POLICY_NAME}\"" "${POLICIES_FILE}"; then
    echo "Ranger bootstrap policy '${PROXY_BOOTSTRAP_POLICY_NAME}' already exists"
else
    curl_post_json "${RANGER_URL}/service/public/v2/api/policy" /bootstrap/nifi-proxy-bootstrap-policy.json

    echo "Created Ranger bootstrap policy '${PROXY_BOOTSTRAP_POLICY_NAME}'"
fi

echo "Ranger bootstrap completed successfully"