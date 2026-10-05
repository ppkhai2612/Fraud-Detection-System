#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python3"

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_DIR="${PROJECT_DIR}/schemas"

echo "Registering Avro schemas with Schema Registry..."

register_schema() {
    local subject="$1"
    local schema_file="$2"

    # Read schema, escape for JSON payload
    local schema
    schema=$($PYTHON -c 'import json,sys; print(json.dumps(sys.stdin.read()))' < "$schema_file")

    echo "  Registering: $subject"
    local response
    # response format:
    # {"id":1}
    # 200
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{\"schema\": $schema}" \
        "$SCHEMA_REGISTRY_URL/subjects/$subject/versions"
    )

    local http_code
    http_code=$(echo "$response" | tail -1)
    local body
    body=$(echo "$response" | head -1)

    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        echo "    OK (id: $(echo "$body" | $PYTHON -c 'import json,sys; print(json.load(sys.stdin)["id"])'))"
    else
        echo "    FAILED ($http_code): $body"
        return 1
    fi
}

# Register a schema that references types from other schemas.
# Resolves references by inlining type definitions from dependency schemas.
register_schema_resolved() {
    local subject="$1"
    local schema_file="$2"
    shift 2
    local ref_files=("$@")

    echo "  Registering: $subject (resolving references)"

    # Use Python to resolve type references and produce a self-contained schema
    # "python -": python reads from stdin that passed by heredoc
    local resolved_schema
    resolved_schema=$($PYTHON - "$schema_file" "${ref_files[@]}" << 'PYEOF'
import json, sys

def extract_named_types(schema):
    """Extract all named types (enums, records) defined in a schema."""
    types = {}
    ns = schema.get("namespace", "")

    def walk(obj, current_ns):
        if isinstance(obj, dict):
            t = obj.get("type")
            if t in ("enum", "record"):
                type_ns = obj.get("namespace", current_ns)
                name = obj["name"]
                fqn = f"{type_ns}.{name}" if type_ns else name
                types[fqn] = obj
            for v in obj.values():
                walk(v, obj.get("namespace", current_ns))
        elif isinstance(obj, list):
            for item in obj:
                walk(item, current_ns)

    walk(schema, ns)
    return types

# Collect all named types from dependency schemas
named_types = {}
for ref_path in sys.argv[2:]:
    with open(ref_path) as f:
        named_types.update(extract_named_types(json.load(f)))
    
# Read the main schema
with open(sys.argv[1]) as f:
    main_schema = json.load(f)

# Resolve: replace type-name references with inline definitions
inlined = set()
def resolve(obj, parent_ns=""):
    if isinstance(obj, str):
        fqn = obj if "." in obj else (f"{parent_ns}.{obj}" if parent_ns else obj)
        if fqn in named_types and fqn not in inlined:
            inlined.add(fqn)
            return resolve(named_types[fqn], parent_ns)
        return obj
    elif isinstance(obj, dict):
        ns = obj.get("namespace", parent_ns)
        return {k: resolve(v, ns) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve(item, parent_ns) for item in obj]
    return obj

resolved = resolve(main_schema, main_schema.get("namespace", ""))
print(json.dumps(resolved))
PYEOF
    )

    # Escape for JSON payload and register
    local escaped
    escaped=$(echo "$resolved_schema" | $PYTHON -c 'import json,sys; print(json.dumps(sys.stdin.read()))')

    local response
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{\"schema\": $escaped}" \
        "$SCHEMA_REGISTRY_URL/subjects/$subject/versions")
    
    local http_code
    http_code=$(echo "$response" | tail -1)
    local body
    body=$(echo "$response" | head -1)
    
    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        echo "    OK (id: $(echo "$body" | $PYTHON -c 'import json,sys; print(json.load(sys.stdin)["id"])'))"
    else
        echo "    FAILED ($http_code): $body"
        return 1
    fi
}

# Self-contained schemas
register_schema "transactions-value" "$SCHEMA_DIR/transaction.avsc"
register_schema "account-updates-value" "$SCHEMA_DIR/account.avsc"
register_schema "merchant-updates-value" "$SCHEMA_DIR/merchant.avsc"

# Schema with cross-references (resolve types from transaction, account, merchant)
register_schema_resolved "enriched-transactions-value" \
    "$SCHEMA_DIR/enriched-transaction.avsc" \
    "$SCHEMA_DIR/transaction.avsc" \
    "$SCHEMA_DIR/account.avsc" \
    "$SCHEMA_DIR/merchant.avsc"

# Self-contained schemas
register_schema "fraud-alerts-value" "$SCHEMA_DIR/fraud-alert.avsc"

echo
echo "Verifying registered subjects..."
curl -s "$SCHEMA_REGISTRY_URL/subjects" | $PYTHON -m json.tool

echo
echo "Schema registration completed."