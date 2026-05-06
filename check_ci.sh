#!/bin/bash
# Poll GitHub CI status with retry
URL="https://api.github.com/repos/zhbxlm/async-scheduler-framework/actions/runs?per_page=1&branch=main"
MAX_RETRIES=10
for i in $(seq 1 $MAX_RETRIES); do
  echo "[Attempt $i] Checking CI..."
  RESP=$(curl -s --connect-timeout 10 --max-time 15 "$URL" 2>/dev/null)
  if echo "$RESP" | grep -q '"conclusion"'; then
    STATUS=$(echo "$RESP" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    CONCLUSION=$(echo "$RESP" | grep -o '"conclusion":"[^"]*"' | head -1 | cut -d'"' -f4)
    RUN_ID=$(echo "$RESP" | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2)
    HTML_URL=$(echo "$RESP" | grep -o '"html_url":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo "Run #$RUN_ID: status=$STATUS conclusion=$CONCLUSION"
    echo "URL: $HTML_URL"
    if [ "$STATUS" = "completed" ]; then
      if [ "$CONCLUSION" = "success" ]; then
        echo "CI PASSED "
        exit 0
      else
        echo "CI FAILED (conclusion=$CONCLUSION)"
        exit 1
      fi
    fi
  elif echo "$RESP" | grep -q 'rate limit'; then
    echo "Rate limited, waiting 60s..."
  else
    echo "Unexpected response: ${RESP:0:200}"
  fi
  sleep 60
done
echo "Timed out after $MAX_RETRIES attempts"
exit 2
