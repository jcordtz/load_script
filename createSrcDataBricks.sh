if [ "$#" -ne 2 ]
then
  echo "Incorrect number of arguments"
  echo "Usage: $0 <source name> <collection_id>"
  exit 1
fi

source_name=$1
collection_id=$2

echo "{"                                                            > work/createSource.json
echo "    \"kind\": \"DatabricksHMS\", "                           >> work/createSource.json
echo "    \"name\": \"$source_name\","                             >> work/createSource.json 
echo "    \"description\": \"Placeholder for $source_name\","      >> work/createSource.json
echo "    \"qaulifiedName\": \"databricks://${source_name}_server\"," >> work/createSource.json
echo "    \"properties\": { "                                      >> work/createSource.json
echo "       \"collection\": { "                                   >> work/createSource.json
echo "            \"type\": \"CollectionReference\", "             >> work/createSource.json
echo "            \"referenceName\": \"$collection_id\""           >> work/createSource.json
echo "       }, "                                                  >> work/createSource.json
echo "       \"subscriptionId\": \"Manual (does not exist)\", "    >> work/createSource.json
echo "       \"resourceGroup\": \"$source_name\", "                >> work/createSource.json
echo "       \"resourceName\": \"$source_name\", "                 >> work/createSource.json
echo "       \"workspaceUrl\": \"$source_name\", "                 >> work/createSource.json
echo "       \"resourceType\": \"WORKSPACE\" "                     >> work/createSource.json
echo "     } "                                                     >> work/createSource.json
echo "} "                                                          >> work/createSource.json

pv scan putDataSource --dataSourceName=$source_name --payloadFile=work/createSource.json
