#!/usr/bin/env bash

set -e

CONFIG_FILE="/etc/trino/config.properties"
CATALOG_DIR="/etc/trino/catalog"

mkdir -p "$CATALOG_DIR"

echo "[ENTRYPOINT] Starting Trino env config processing..." >&2

# Function to merge a property into a file
merge_property() {
    local file="$1"
    local key="$2"
    local value="$3"

    if grep -q "^$key=" "$file" 2>/dev/null; then
        echo "[MERGE] Updating $key=$value in $file" >&2
        tmpfile="$(mktemp /tmp/merge.XXXXXX)"
        sed "s|^$key=.*|$key=$value|" "$file" > "$tmpfile"
        cat "$tmpfile" > "$file"
        rm -f "$tmpfile"
    else
        echo "[MERGE] Adding $key=$value to $file" >&2
        echo "$key=$value" >> "$file"
    fi
}


# Convert string to lowercase
to_lower() {
    echo "$1" | tr '[:upper:]' '[:lower:]'
}

# Process TRINO_CONFIG_*
echo "[ENTRYPOINT] Processing TRINO_CONFIG_* variables..." >&2
for var in $(env | grep '^TRINO_CONFIG_' | awk -F= '{print $1}'); do
    value="${!var}"
    key="${var#TRINO_CONFIG_}"
    key="${key//__/-}"
    key="${key//_/.}"
    key="$(to_lower "$key")"
    echo "[CONFIG] $var -> $key=$value" >&2
    merge_property "$CONFIG_FILE" "$key" "$value"
done

# Process TRINO_CATALOG_<catalog>_*
echo "[ENTRYPOINT] Processing TRINO_CATALOG_* variables..." >&2
for var in $(env | grep '^TRINO_CATALOG_' | awk -F= '{print $1}'); do
    value="${!var}"
    rest="${var#TRINO_CATALOG_}"
    catalog_name="${rest%%_*}"
    catalog_name="$(to_lower "$catalog_name")"
    prop="${rest#${rest%%_*}_}"
    prop="${prop//__/-}"
    prop="${prop//_/.}"
    prop="$(to_lower "$prop")"
    file="$CATALOG_DIR/$catalog_name.properties"
    touch "$file"
    echo "[CATALOG] $var -> $file: $prop=$value" >&2
    merge_property "$file" "$prop" "$value"
done

echo "[ENTRYPOINT] Finished processing environment variables." >&2

# Execute the original CMD
exec "$@"
