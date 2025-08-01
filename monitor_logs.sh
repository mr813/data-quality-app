#!/bin/bash

# Simple Log Monitor for Data Quality App
echo "🔍 Data Quality App Log Monitor"
echo "Started at: $(date)"
echo "Process ID: 25672"
echo "Press Ctrl+C to stop monitoring"
echo ""

while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Status Check ==="
    
    # Check if process is running
    if ps -p 25672 > /dev/null 2>&1; then
        echo "✅ Process running (PID: 25672)"
    else
        echo "❌ Process not found"
        break
    fi
    
    # Check health
    HEALTH=$(curl -s http://localhost:8501/_stcore/health 2>/dev/null)
    if [ "$HEALTH" = "ok" ]; then
        echo "✅ Health check: OK"
    else
        echo "❌ Health check: Failed"
    fi
    
    # Check connections
    CONNECTIONS=$(lsof -i :8501 | grep ESTABLISHED | wc -l)
    echo "🌐 Active connections: $CONNECTIONS"
    
    # Check performance
    PERF=$(ps -p 25672 -o %cpu,%mem --no-headers 2>/dev/null)
    if [ ! -z "$PERF" ]; then
        echo "📊 Performance: $PERF (CPU%, Memory%)"
    fi
    
    echo ""
    sleep 5
done 