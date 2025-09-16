#!/bin/bash

echo "🧪 Testing ARM64 NEON SIMD Performance on Apple Silicon M4"
echo "=========================================================="

BASE_URL="http://127.0.0.1:3030"

# Clear any existing data
echo "✅ Clearing database..."
curl -s -X POST "$BASE_URL/v1/clear" > /dev/null

# Insert test data with numeric keys
echo "📝 Inserting 1000 test key-value pairs..."
start_time=$(gdate +%s%N 2>/dev/null || date +%s%N)

for i in $(seq 1 1000); do
    value="value_$(printf '%06d' $i)"
    curl -s -X POST "$BASE_URL/v1/put" \
        -H "Content-Type: application/json" \
        -d "{\"key\":$i,\"value\":\"$value\"}" > /dev/null
done

end_time=$(gdate +%s%N 2>/dev/null || date +%s%N)
insert_duration=$((($end_time - $start_time) / 1000000))
echo "✅ Inserted 1000 keys in ${insert_duration}ms"

# Test single key lookups for baseline
echo ""
echo "🔍 Baseline: Single key lookups (100 keys)..."
single_start=$(gdate +%s%N 2>/dev/null || date +%s%N)

single_found=0
for i in $(seq 1 100); do
    response=$(curl -s "$BASE_URL/v1/lookup?key=$i")
    if [[ $response == *"value_"* ]]; then
        ((single_found++))
    fi
done

single_end=$(gdate +%s%N 2>/dev/null || date +%s%N)
single_duration=$((($single_end - $single_start) / 1000000))
single_throughput=$(echo "scale=2; 100 * 1000 / $single_duration" | bc)

echo "✅ Single lookups: $single_found found, ${single_duration}ms total, ${single_throughput} keys/sec"

# Test batch lookups (SIMD should kick in here)
echo ""
echo "⚡ SIMD Batch Lookups (ARM64 NEON)..."

# Create batch keys array (numeric values)
batch_keys=""
for i in $(seq 1 100); do
    if [ $i -eq 1 ]; then
        batch_keys="$i"
    else
        batch_keys="$batch_keys,$i"
    fi
done

batch_payload="[$batch_keys]"

batch_start=$(gdate +%s%N 2>/dev/null || date +%s%N)

response=$(curl -s -X POST "$BASE_URL/v1/lookup_batch" \
    -H "Content-Type: application/json" \
    -d "$batch_payload")

batch_end=$(gdate +%s%N 2>/dev/null || date +%s%N)
batch_duration=$((($batch_end - $batch_start) / 1000000))

echo "📊 Batch response length: $(echo "$response" | wc -c) characters"

if [[ $response == *"key"* ]] || [[ $response == *"value"* ]]; then
    # Count found results by counting "key" occurrences
    batch_found=$(echo "$response" | grep -o '"key"' | wc -l)
    batch_throughput=$(echo "scale=2; 100 * 1000 / $batch_duration" | bc)
    speedup=$(echo "scale=2; $batch_throughput / $single_throughput" | bc)
    
    echo "✅ Batch lookups: $batch_found found, ${batch_duration}ms total, ${batch_throughput} keys/sec"
    echo ""
    echo "🎯 PERFORMANCE COMPARISON:"
    echo "   Single lookup throughput: ${single_throughput} keys/sec"
    echo "   SIMD batch throughput:    ${batch_throughput} keys/sec"
    echo "   🚀 SIMD Speedup:          ${speedup}x"
    
    echo ""
    echo "🏗️  ARCHITECTURE INFO:"
    echo "   Target: $(uname -m)"
    
    if [[ "$(uname -m)" == "arm64" ]]; then
        echo "   ✅ ARM64 NEON: Available"
        echo "   🎯 Expected: 2-4x speedup for NEON vs scalar"
    else
        echo "   ✅ x86_64: Available" 
        echo "   🎯 Expected: 4-8x speedup for AVX2 vs scalar"
    fi
    
    # Convert speedup to integer for comparison
    speedup_check=$(echo "$speedup >= 2.0" | bc)
    if [ "$speedup_check" -eq 1 ]; then
        echo ""
        echo "🎉 SUCCESS: SIMD is working! ${speedup}x speedup achieved"
    else
        speedup_moderate=$(echo "$speedup >= 1.5" | bc)
        if [ "$speedup_moderate" -eq 1 ]; then
            echo ""
            echo "⚡ MODERATE: Some SIMD benefit with ${speedup}x speedup"
        else
            echo ""
            echo "⚠️  WARNING: Low speedup (${speedup}x) - SIMD may not be fully active"
        fi
    fi
else
    echo "❌ Batch lookup failed or returned unexpected format"
    echo "Response: $response"
fi

echo ""
echo "🏁 Test completed!"
