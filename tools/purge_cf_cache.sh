#!/bin/bash

# ==========================================
# Cloudflare Zone Manager (Cache & Dev Mode)
# ==========================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    . "$ENV_FILE"
    set +a
fi

# --- Cloudflare Credentials ---
# Prefer environment variables so secrets are not stored in the script.
API_TOKEN="${CLOUDFLARE_API_TOKEN:-}"
ZONE_ID="${CLOUDFLARE_ZONE_ID:-}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 2. CHECK CREDENTIALS
if [[ -z "$ZONE_ID" ]]; then
    echo -e "${YELLOW}CLOUDFLARE_ZONE_ID is not set.${NC}"
    read -p "Enter Cloudflare Zone ID: " ZONE_ID
fi

if [[ -z "$API_TOKEN" ]]; then
    echo -e "${YELLOW}CLOUDFLARE_API_TOKEN is not set.${NC}"
    read -s -p "Enter Cloudflare API Token: " API_TOKEN
    echo ""
fi

if [[ -z "$ZONE_ID" || -z "$API_TOKEN" ]]; then
    echo -e "${RED}Error: Zone ID and API Token are required.${NC}"
    exit 1
fi

# 3. MAIN MENU
echo -e "\n${BLUE}--- Cloudflare Management ---${NC}"
echo "1) Purge Cache: EVERYTHING"
echo "2) Purge Cache: Specific URLs"
echo "3) Development Mode: Manage (On/Off/Status)"
read -p "Select Option [1-3]: " MAIN_SELECTION

# Base API URL
BASE_URL="https://api.cloudflare.com/client/v4/zones/$ZONE_ID"

# --------------------------------------
# LOGIC HANDLERS
# --------------------------------------

if [[ "$MAIN_SELECTION" == "1" ]]; then
    # --- PURGE ALL ---
    echo -e "\n${YELLOW}Purging ALL cached files...${NC}"
    ENDPOINT="$BASE_URL/purge_cache"
    DATA='{"purge_everything":true}'
    METHOD="POST"

elif [[ "$MAIN_SELECTION" == "2" ]]; then
    # --- PURGE URLS ---
    echo -e "\n${YELLOW}Enter URLs to purge (space separated):${NC}"
    read -p "URLs: " URL_INPUT

    if [[ -z "$URL_INPUT" ]]; then
        echo -e "${RED}No URLs provided. Exiting.${NC}" ; exit 1
    fi

    # Format URLs for JSON
    FORMATTED_URLS=$(echo $URL_INPUT | sed 's/ /","/g')
    DATA="{\"files\":[\"$FORMATTED_URLS\"]}"
    ENDPOINT="$BASE_URL/purge_cache"
    METHOD="POST"
    echo -e "${YELLOW}Purging specific files...${NC}"

elif [[ "$MAIN_SELECTION" == "3" ]]; then
    # --- DEVELOPMENT MODE ---
    echo -e "\n${BLUE}--- Development Mode ---${NC}"
    echo "1) Check Status"
    echo "2) Turn ON (Bypass cache for 3 hours)"
    echo "3) Turn OFF"
    read -p "Selection: " DEV_SELECTION

    ENDPOINT="$BASE_URL/settings/development_mode"

    if [[ "$DEV_SELECTION" == "1" ]]; then
        METHOD="GET"
        DATA="" # No body for GET
    elif [[ "$DEV_SELECTION" == "2" ]]; then
        METHOD="PATCH"
        DATA='{"value":"on"}'
        echo -e "${YELLOW}Enabling Development Mode...${NC}"
    elif [[ "$DEV_SELECTION" == "3" ]]; then
        METHOD="PATCH"
        DATA='{"value":"off"}'
        echo -e "${YELLOW}Disabling Development Mode...${NC}"
    else
        echo -e "${RED}Invalid selection.${NC}" ; exit 1
    fi

else
    echo -e "${RED}Invalid selection.${NC}"
    exit 1
fi

# --------------------------------------
# EXECUTE REQUEST
# --------------------------------------

# Construct curl command based on method
if [[ "$METHOD" == "GET" ]]; then
    RESPONSE=$(curl -s -X GET "$ENDPOINT" \
        -H "Authorization: Bearer $API_TOKEN" \
        -H "Content-Type: application/json")
else
    RESPONSE=$(curl -s -X "$METHOD" "$ENDPOINT" \
        -H "Authorization: Bearer $API_TOKEN" \
        -H "Content-Type: application/json" \
        -d "$DATA")
fi

# --------------------------------------
# PARSE RESULTS
# --------------------------------------

if command -v jq &> /dev/null; then
    SUCCESS=$(echo "$RESPONSE" | jq -r '.success')

    if [[ "$SUCCESS" == "true" ]]; then
        echo -e "${GREEN}✔ Success!${NC}"

        # Specific output for Dev Mode
        if [[ "$MAIN_SELECTION" == "3" ]]; then
            VAL=$(echo "$RESPONSE" | jq -r '.result.value')
            TIME=$(echo "$RESPONSE" | jq -r '.result.time_remaining')
            echo -e "Current Status: ${BLUE}$VAL${NC}"
            if [[ "$VAL" == "on" ]]; then
                # Convert seconds to minutes roughly
                MINS=$((TIME / 60))
                echo -e "Time Remaining: ${BLUE}$TIME seconds (~$MINS mins)${NC}"
            fi
        else
            # Output for Cache Purge
            ID=$(echo "$RESPONSE" | jq -r '.result.id')
            echo "Request ID: $ID"
        fi
    else
        echo -e "${RED}✘ Error occurred.${NC}"
        echo "$RESPONSE" | jq '.errors[] | .message'
    fi
else
    # Fallback if jq is not installed
    echo -e "\nResponse:"
    echo "$RESPONSE"
fi
