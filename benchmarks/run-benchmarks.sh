#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# ZBank Benchmark Runner (Linux / macOS / Git Bash)
# ==============================================================================

PROFILE="${1:-fast}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "================================================================="
echo " ZBank Benchmark Suite (Profile: $PROFILE, 3 Runs + 30s Warm-Up)"
echo "================================================================="

cd "$REPO_ROOT"

echo -e "\n[1/4] Ensuring Docker infrastructure is up..."
docker compose up -d postgres-db redis-cache kafka
sleep 5

echo -e "\n[2/4] Building production JAR..."
mvn clean package -DskipTests -q

JAR_PATH="$REPO_ROOT/target/fraud-velocity-engine-0.0.1-SNAPSHOT.jar"

scenarios=(
    "VT ON  + Lua (Atomic)|true|LUA|true"
    "VT ON  + Pipelined|true|PIPELINED|true"
    "VT ON  + Non-Pipelined|true|NON_PIPELINED|false"
    "VT OFF + Lua (Atomic)|false|LUA|true"
    "VT OFF + Pipelined|false|PIPELINED|true"
    "VT OFF + Non-Pipelined|false|NON_PIPELINED|false"
)

for item in "${scenarios[@]}"; do
    IFS="|" read -r name vt mode pipe <<< "$item"
    echo -e "\n-----------------------------------------------------------------"
    echo " Running Scenario: $name (VT=$vt, Mode=$mode, Pipe=$pipe)"
    echo "-----------------------------------------------------------------"

    # Ensure clean Redis state
    docker exec zbank-redis redis-cli FLUSHDB > /dev/null 2>&1 || true
    sleep 1

    JVM_ARGS="-Xms64m -Xmx384m -Xss512k -Dspring.threads.virtual.enabled=$vt -Dzbank.redis.velocity.mode=$mode -Dzbank.redis.pipelining.enabled=$pipe -Dspring.jpa.show-sql=false -Dlogging.level.root=WARN -Dlogging.level.com.zbank=INFO"
    java $JVM_ARGS -jar "$JAR_PATH" > /dev/null 2>&1 &
    APP_PID=$!

    echo -n "Waiting for application to boot..."
    for i in {1..30}; do
        if curl -s http://localhost:8080/actuator/health | grep -q '"status":"UP"'; then
            echo " UP!"
            break
        fi
        sleep 2
        echo -n "."
    done

    # 30s warm-up
    echo "Executing 30-second unmeasured warm-up..."
    docker run --rm -i --network host -e PROFILE="warmup" -e BASE_URL="http://localhost:8080" grafana/k6 run - < "$SCRIPT_DIR/load-test.js" > /dev/null 2>&1 || true

    # 3 measured runs
    for run_idx in 1 2 3; do
        echo "  [Run $run_idx of 3] Executing k6 benchmark..."
        docker run --rm -i --network host -e PROFILE="$PROFILE" -e BASE_URL="http://localhost:8080" grafana/k6 run - < "$SCRIPT_DIR/load-test.js"
        sleep 2
    done

    echo "Stopping application (PID $APP_PID)..."
    kill -9 "$APP_PID" 2>/dev/null || true
    sleep 3
done

echo -e "\nBenchmark execution complete."
