if [ "$#" -ne 2 ]
then
  echo "Incorrect number of arguments"
  echo "Usage: $0 <source name> <collection_id>"
  exit 1
fi

source_name=$1
collection_id=$2

echo "{"                                                                              > work/createSource.json
echo "    \"kind\": \"AzureSqlDatabase\","                                           >> work/createSource.json
echo "    \"attributes\": {"                                                         >> work/createSource.json
echo "      \"qaulifiedName\": \"mssql://${source_name}/${source_name_db}\","        >> work/createSource.json
echo "      \"name\": \"${source_name}\","                                           >> work/createSource.json
echo "      \"description\": \"Placeholder for ${source_name}\","                    >> work/createSource.json
echo "      \"serverName\": \"mssql://${source_name}\""                              >> work/createSource.json
echo "        },"                                                                    >> work/createSource.json
echo "    \"properties\": { "                                                        >> work/createSource.json
echo "      \"serverEndpoint\": \"${source_name}.database.windows.net\"",            >> work/createSource.json
echo "       \"collection\": { "                                                     >> work/createSource.json
echo "            \"type\": \"CollectionReference\", "                               >> work/createSource.json
echo "            \"referenceName\": \"$collection_id\""                             >> work/createSource.json
echo "       } "                                                                     >> work/createSource.json
echo "     } "                                                                       >> work/createSource.json
echo "} "                                                                            >> work/createSource.json

pv scan putDataSource --dataSourceName=$source_name --payloadFile=work/createSource.json
