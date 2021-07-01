if [ -z $1 ]; then
    echo "input job id in format: sh submit_control.sh [job id]"
    exit
fi
JOB_ID=$1
echo "job id is $JOB_ID"

ClASS_NAME="org.apache.flink.streaming.controlplane.udm.FraudDetectionController"
CLASS_FILE="FraudDetectionController.java"
CONTROLLER_ID="fraud_detector"

SOURCE_CODE_URL=/home/hya/prog/flink-nus/flink-streaming-java/src/main/java/org/apache/flink/streaming/controlplane/udm/FraudDetectionController.java

JSON='{"className"':\"$ClASS_NAME\",'"classFile"':\"$CLASS_FILE\",'"controllerID"':\"$CONTROLLER_ID\"'}'
echo "request=$JSON\n"

curl --form "fileupload=@$SOURCE_CODE_URL" http://127.0.0.1:8520/jobs/$JOB_ID/smcontroller -F "request=$JSON" || exit 1

echo "\ncontroller submitted"
