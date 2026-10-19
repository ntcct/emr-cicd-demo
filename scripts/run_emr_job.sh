#!/usr/bin/env bash
# Submit the Spark job to EMR Serverless, wait for completion, verify output,
# and publish job id / cost / console pointers to the GitHub job summary.
set -euo pipefail

: "${ENV_NAME:?}" "${AWS_REGION:?}" "${EMR_APP_ID:?}" "${EMR_JOB_ROLE_ARN:?}" "${S3_BUCKET:?}"
SHA="${GITHUB_SHA:-local}"
SHORT_SHA="${SHA:0:7}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"
OUTPUT_FILE="${GITHUB_OUTPUT:-/dev/null}"
TIMEOUT_MIN="${TIMEOUT_MIN:-30}"

ARTIFACTS="s3://${S3_BUCKET}/artifacts/${SHA}"
INPUT="s3://${S3_BUCKET}/data/sales.csv"
OUTPUT="s3://${S3_BUCKET}/${ENV_NAME}/output/${SHORT_SHA}/"
LOG_URI="s3://${S3_BUCKET}/logs/"

# Smallest possible footprint: 1 driver + 1 executor, 1 vCPU each, no dynamic allocation.
SPARK_PARAMS="--py-files ${ARTIFACTS}/sales_pipeline.zip \
--conf spark.driver.cores=1 --conf spark.driver.memory=2g \
--conf spark.executor.cores=1 --conf spark.executor.memory=2g \
--conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false \
--conf spark.emr-serverless.driver.disk=20g --conf spark.emr-serverless.executor.disk=20g"

JOB_DRIVER=$(jq -n \
  --arg entry "${ARTIFACTS}/main.py" --arg in "$INPUT" --arg out "$OUTPUT" \
  --arg env "$ENV_NAME" --arg params "$SPARK_PARAMS" \
  '{sparkSubmit:{entryPoint:$entry,entryPointArguments:["--input",$in,"--output",$out,"--env",$env],sparkSubmitParameters:$params}}')

CONFIG=$(jq -n --arg log "$LOG_URI" \
  '{monitoringConfiguration:{s3MonitoringConfiguration:{logUri:$log}}}')

echo "Submitting job: env=${ENV_NAME} app=${EMR_APP_ID} sha=${SHORT_SHA}"
JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --region "$AWS_REGION" \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_JOB_ROLE_ARN" \
  --name "sales-${ENV_NAME}-${SHORT_SHA}" \
  --execution-timeout-minutes "$TIMEOUT_MIN" \
  --job-driver "$JOB_DRIVER" \
  --configuration-overrides "$CONFIG" \
  --tags "env=${ENV_NAME},commit=${SHORT_SHA}" \
  --query jobRunId --output text)

echo "job_run_id=${JOB_RUN_ID}" >> "$OUTPUT_FILE"
echo "output_path=${OUTPUT}" >> "$OUTPUT_FILE"
echo "JobRunId: ${JOB_RUN_ID}"

# Cancel the EMR job if the GitHub job is cancelled / times out (avoid paying for orphans).
cleanup() {
  local rc=$?
  if [[ $rc -ne 0 && -n "${STATE:-}" && ! "$STATE" =~ ^(SUCCESS|FAILED|CANCELLED)$ ]]; then
    echo "Cancelling job ${JOB_RUN_ID}"
    aws emr-serverless cancel-job-run --region "$AWS_REGION" \
      --application-id "$EMR_APP_ID" --job-run-id "$JOB_RUN_ID" || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

DEADLINE=$(( $(date +%s) + TIMEOUT_MIN * 60 ))
STATE="SUBMITTED"
while true; do
  RUN_JSON=$(aws emr-serverless get-job-run --region "$AWS_REGION" \
    --application-id "$EMR_APP_ID" --job-run-id "$JOB_RUN_ID" --output json)
  STATE=$(jq -r '.jobRun.state' <<<"$RUN_JSON")
  echo "$(date -u +%H:%M:%S) state=${STATE}"
  case "$STATE" in
    SUCCESS|FAILED|CANCELLED) break ;;
  esac
  if (( $(date +%s) > DEADLINE )); then
    echo "::error::Timed out waiting for job ${JOB_RUN_ID}"; exit 1
  fi
  sleep 20
done

BILLED=$(jq -r '.jobRun.billedResourceUtilization // {} |
  "vCPU-h=\(.vCPUHour // "n/a")  memGB-h=\(.memoryGBHour // "n/a")  storageGB-h=\(.storageGBHour // "n/a")"' <<<"$RUN_JSON")
DETAILS=$(jq -r '.jobRun.stateDetails // ""' <<<"$RUN_JSON")
DRIVER_LOG="${LOG_URI}applications/${EMR_APP_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/stdout.gz"
CONSOLE="https://${AWS_REGION}.console.aws.amazon.com/emr/home?region=${AWS_REGION}#/serverless"

{
  echo "### EMR Serverless job - \`${ENV_NAME}\`"
  echo "| Item | Value |"
  echo "|---|---|"
  echo "| State | **${STATE}** ${DETAILS} |"
  echo "| Job run ID | \`${JOB_RUN_ID}\` |"
  echo "| Application ID | \`${EMR_APP_ID}\` |"
  echo "| Output | \`${OUTPUT}\` |"
  echo "| Driver stdout | \`${DRIVER_LOG}\` |"
  echo "| Billed usage | ${BILLED} |"
  echo "| Console | [EMR Serverless](${CONSOLE}) -> Manage applications -> \`${EMR_APP_ID}\` -> Job runs |"
} >> "$SUMMARY"

if [[ "$STATE" != "SUCCESS" ]]; then
  echo "::error::EMR job ${JOB_RUN_ID} ended in ${STATE}: ${DETAILS}"
  echo "Driver stderr: ${LOG_URI}applications/${EMR_APP_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/stderr.gz"
  exit 1
fi

# Post-deploy verification: Spark writes _SUCCESS only on a committed output.
aws s3 ls "${OUTPUT}_SUCCESS" --region "$AWS_REGION" >/dev/null \
  || { echo "::error::_SUCCESS marker missing in ${OUTPUT}"; exit 1; }

echo "Driver output (result table):"
aws s3 cp "$DRIVER_LOG" - --region "$AWS_REGION" 2>/dev/null | gunzip | tail -n 20 || true
echo "Job ${JOB_RUN_ID} succeeded. ${BILLED}"
