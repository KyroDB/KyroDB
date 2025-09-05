#!/bin/bash

echo "Testing KyroDB routes..."

# Basic health check
echo "1. Health check:"
curl -s -H "Accept-Encoding: gzip" http://127.0.0.1:8080/health | gunzip
echo -e "\n"

# Test v1 endpoints
echo "2. V1 GET lookup:"
curl -s -H "Accept-Encoding: gzip" "http://127.0.0.1:8080/v1/lookup?key=123" | gunzip
echo -e "\n"

echo "3. V1 POST put:"
curl -s -H "Accept-Encoding: gzip" -H "Content-Type: application/json" -X POST -d '{"key": 456, "value": "test"}' http://127.0.0.1:8080/v1/put | gunzip
echo -e "\n"

# Test v2 endpoints
echo "4. V2 GET collections:"
curl -s -H "Accept-Encoding: gzip" -X GET http://127.0.0.1:8080/v2/collections | gunzip
echo -e "\n"

echo "5. V2 POST collections (minimal):"
curl -s -H "Accept-Encoding: gzip" -H "Content-Type: application/json" -X POST -d '{"name": "test"}' http://127.0.0.1:8080/v2/collections | gunzip
echo -e "\n"

echo "6. V2 POST collections (full):"
curl -s -H "Accept-Encoding: gzip" -H "Content-Type: application/json" -X POST -d '{
  "name": "test_collection",
  "description": "Test collection",
  "schema": {
    "fields": {
      "title": {
        "field_type": "String",
        "indexed": true,
        "required": false
      }
    }
  }
}' http://127.0.0.1:8080/v2/collections | gunzip
echo -e "\n"

# Test search endpoints
echo "7. V2 POST search/vector:"
curl -s -H "Accept-Encoding: gzip" -H "Content-Type: application/json" -X POST -d '{
  "collection": "test",
  "vector": [1.0, 2.0, 3.0],
  "k": 5
}' http://127.0.0.1:8080/v2/search/vector | gunzip
echo -e "\n"

echo "Route testing complete!"
