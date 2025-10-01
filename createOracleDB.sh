if [ "$#" -ne 4 ]
then
  echo "Incorrect number of arguments"
  echo "Usage: $0 <domain_id> <collection_id> <source name> <input_file>"
  exit 1
fi

domain_id=$1
collection_id=$2
source_name=$3
input_file=$4
log_file=work/${3}_$$_log.sh

echo
echo "Creating Oracle Database Purview entries"
echo
echo "All processing is registered in ** ${log_file} ** with corresponding delete statements"
echo
echo
echo "NOTE:"
echo "Statements in this file can be execute one-by-one or the complete file"
echo "can be run to remove all that has been done in this job"
echo

echo "# Oracle Server/Database creation - job data `date`" >  ${log_file}
chmod +x ${log_file}

echo " " >> ${log_file}
echo "# NOTE:" >> ${log_file}
echo "# Statements in this file can be execute one-by-one or the complete file" >> ${log_file}
echo "# can be run to remove all that has been done in this job" >> ${log_file}
echo " " >> ${log_file}

echo "Processing server - ${source_name}"
echo "# Server - ${source_name}" >> ${log_file}

echo "{"                                                                                          > work/OracleServer.json
echo "    \"entity\": {"                                                                         >> work/OracleServer.json
echo "        \"attributes\": {"                                                                 >> work/OracleServer.json
echo "            \"description\": \"Manual Oracle server named ${source_nane}.\","              >> work/OracleServer.json
echo "            \"name\": \"${source_nane}\","                                                 >> work/OracleServer.json
echo "            \"qualifiedName\": \"oracle://${source_name}\""                                >> work/OracleServer.json
echo "        },"                                                                                >> work/OracleServer.json
echo "        \"collection\": {"                                                                 >> work/OracleServer.json
echo "            \"type\": \"CollectionReference\","                                            >> work/OracleServer.json
echo "            \"referenceName\": \"$collection_id\""                                         >> work/OracleServer.json
echo "        },"                                                                                >> work/OracleServer.json
echo "        \"collectionId\": \"$collection_id\","                                             >> work/OracleServer.json
echo "        \"domainId\": \"$domain_id\","                                                     >> work/OracleServer.json
echo "        \"typeName\": \"oracle_server\""                                                   >> work/OracleServer.json
echo "    }"                                                                                     >> work/OracleServer.json
echo "}"                                                                                         >> work/OracleServer.json

pv entity create --payloadFile=work/OracleServer.json > work/temp0
cat work/temp0 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

echo " " >>  ${log_file}
echo "# Oracle Schema creation " >>  ${log_file}

# find and load schemas 

cat $input_file | 
cut -d"," -f1 |
sort -u |
while read schema
do

    if [ "$schema" == "table_schema" ]
    then
       continue
    fi

    echo "Processing schema - ${source_name}/${schema}"
    echo "# Schema : ${source_name}/${schema}" >>  ${log_file}

    echo "{"                                                                                      > work/OracleSchema.json
    echo "\"entity\": "                                                                          >> work/OracleSchema.json
    echo "   { "                                                                                 >> work/OracleSchema.json
    echo "      \"typeName\": \"oracle_schema\", "                                               >> work/OracleSchema.json
    echo "      \"attributes\": { "                                                              >> work/OracleSchema.json
    echo "            \"name\": \"${schema}\", "                                                 >> work/OracleSchema.json
    echo "            \"qualifiedName\": \"oracle://${source_name}/${schema}\", "                >> work/OracleSchema.json
    echo "            \"description\": \"Manual Schema ${schema} for Oracle db\", "              >> work/OracleSchema.json
    echo "            \"db\": { "                                                                >> work/OracleSchema.json
    echo "               \"typeName\": \"oracle_server\", "                                      >> work/OracleSchema.json
    echo "               \"uniqueAttributes\": { "                                               >> work/OracleSchema.json
    echo "                  \"qualifiedName\": \"oracle://{source_name}\" "                      >> work/OracleSchema.json
    echo "                 } "                                                                   >> work/OracleSchema.json
    echo "         },"                                                                           >> work/OracleSchema.json
    echo "        \"relationshipAttributes\": {"                                                 >> work/OracleSchema.json
    echo "            \"oracle_server\": {"                                                      >> work/OracleSchema.json
    echo "            \"qualifiedName\": \"oracle://${source_name}\" "                           >> work/OracleSchema.json
    echo "            }"                                                                         >> work/OracleSchema.json
    echo "         } "                                                                           >> work/OracleSchema.json
    echo "    } "                                                                                >> work/OracleSchema.json
    echo "  } "                                                                                  >> work/OracleSchema.json
    echo "} "                                                                                    >> work/OracleSchema.json

    pv entity create --payloadFile=work/OracleSchema.json > work/temp1
    cat work/temp1 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}


    echo "Processing relationship - ${source_name}/$schema"

    echo "{   "                                                                                  > work/oracleSchemaRelation.json
    echo "    \"end1\": {  "                                                                    >> work/oracleSchemaRelation.json
    echo "        \"typeName\": \"oracle_server\",  "                                           >> work/oracleSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/oracleSchemaRelation.json
    echo "            \"qualifiedName\": \"oracle://${source_name}\" "                          >> work/OracleSchemaRelation.json
    echo "        }  "                                                                          >> work/oracleSchemaRelation.json
    echo "    },  "                                                                             >> work/oracleSchemaRelation.json
    echo "    \"end2\": {  "                                                                    >> work/oracleSchemaRelation.json
    echo "        \"typeName\": \"oracle_schema\",  "                                           >> work/oracleSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/oracleSchemaRelation.json
    echo "            \"qualifiedName\": \"oracle://${source_name}/${schema}\" "                >> work/oracleSchemaRelation.json
    echo "        }  "                                                                          >> work/oracleSchemaRelation.json
    echo "    },  "                                                                             >> work/oracleSchemaRelation.json
    echo "    \"typeName\": \"oracle_server_schemas\"  "                                        >> work/oracleSchemaRelation.json
    echo "}  "                                                                                  >> work/oracleSchemaRelation.json

    pv relationship create --payloadFile=work/OracleSchemaRelation.json > work/temp2
    cat work/temp2 | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done

echo " " >>  ${log_file}
echo "# Oracle tables creation " >>  ${log_file}

# find and load tables 


cat $input_file |
while read line
do

    schema=`echo $line | cut -d"," -f1`
    table=`echo $line | cut -d"," -f2`

    if [ "$schema" == "table_schema" ]
    then
       continue
    fi

    echo "Processing table - $schema.$table"
    echo "# Table - $schema.$table" >> ${log_file}


    echo "{"                                                                                            > work/OracleTable.json
    echo "  \"entity\": "                                                                              >> work/OracleTable.json
    echo "     {"                                                                                      >> work/OracleTable.json
    echo "      \"kind\": \"oracle_table\", "                                                          >> work/OracleTable.json
    echo "      \"typeName\": \"oracle_table\", "                                                      >> work/OracleTable.json
    echo "      \"attributes\": { "                                                                    >> work/OracleTable.json
    echo "        \"name\": \"${table}\", "                                                            >> work/OracleTable.json
    echo "        \"qualifiedName\": \"oracle://${source_name}/${schema}/${table}\", "                 >> work/OracleTable.json
    echo "        \"owner\": null, "                                                                   >> work/OracleTable.json
    echo "        \"description\": \"Inserted using script\" "                                         >> work/OracleTable.json
    echo "     },"                                                                                     >> work/OracleTable.json
    echo "      \"collectionId\": \"${collection_id}\", "                                              >> work/OracleTable.json
    echo "      \"displayText\": \"${table}\", "                                                       >> work/OracleTable.json
    echo "      \"domainId\": \"${domain_id}\" "                                                       >> work/OracleTable.json
    echo "   }"                                                                                        >> work/OracleTable.json
    echo "}"                                                                                           >> work/OracleTable.json

    pv entity create --payloadFile=work/OracleTable.json > work/temp3
    cat work/temp3 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

    echo "Processing relationship - $schema.$table"

    echo "{  "                                                                                           > work/OracleTableRelation.json
    echo "    \"end1\": { "                                                                             >> work/OracleTableRelation.json
    echo "        \"typeName\": \"oracle_schema\", "                                                    >> work/OracleTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                             >> work/OracleTableRelation.json
    echo "             \"qualifiedName\": \"oracle://${source_name}/${schema}\" "                       >> work/OracleTableRelation.json
    echo "        } "                                                                                   >> work/OracleTableRelation.json
    echo "    }, "                                                                                      >> work/OracleTableRelation.json
    echo "    \"end2\": { "                                                                             >> work/OracleTableRelation.json
    echo "        \"typeName\": \"oracle_table\", "                                                     >> work/OracleTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                             >> work/OracleTableRelation.json
    echo "            \"qualifiedName\": \"oracle://${source_name}/${schema}/${table}\" "               >> work/OracleTableRelation.json
    echo "        } "                                                                                   >> work/OracleTableRelation.json
    echo "    }, "                                                                                      >> work/OracleTableRelation.json
    echo "    \"typeName\": \"oracle_schema_tables\" "                                                  >> work/OracleTableRelation.json
    echo "} "                                                                                           >> work/OracleTableRelation.json

    pv relationship create --payloadFile=work/OracleTableRelation.json > work/temp4
    cat work/temp4  | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done
