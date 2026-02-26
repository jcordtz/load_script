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
echo "Creating Mysql Database Purview entries"
echo
echo "All processing is registered in ** ${log_file} ** with corresponding delete statements"
echo
echo
echo "NOTE:"
echo "Statements in this file can be execute one-by-one or the complete file"
echo "can be run to remove all that has been done in this job"
echo

echo "# Mysql Server/Database creation - job data `date`" >  ${log_file}
chmod +x ${log_file}

echo " " >> ${log_file}
echo "# NOTE:" >> ${log_file}
echo "# Statements in this file can be execute one-by-one or the complete file" >> ${log_file}
echo "# can be run to remove all that has been done in this job" >> ${log_file}
echo " " >> ${log_file}

echo "Processing server - ${source_name}"
echo "# Server - ${source_name}" >> ${log_file}

echo "{"                                                                                          > work/MysqlServer.json
echo "    \"entity\": {"                                                                         >> work/MysqlServer.json
echo "        \"attributes\": {"                                                                 >> work/MysqlServer.json
echo "            \"description\": \"Manual Mysql server named ${source_name}.\","               >> work/MysqlServer.json
echo "            \"name\": \"${source_name}\","                                                 >> work/MysqlServer.json
echo "            \"qualifiedName\": \"mysql://${source_name}\""                                 >> work/MysqlServer.json
echo "        },"                                                                                >> work/MysqlServer.json
echo "        \"collection\": {"                                                                 >> work/MysqlServer.json
echo "            \"type\": \"CollectionReference\","                                            >> work/MysqlServer.json
echo "            \"referenceName\": \"$collection_id\""                                         >> work/MysqlServer.json
echo "        },"                                                                                >> work/MysqlServer.json
echo "        \"collectionId\": \"$collection_id\","                                             >> work/MysqlServer.json
echo "        \"domainId\": \"$domain_id\","                                                     >> work/MysqlServer.json
echo "        \"typeName\": \"mysql_server\""                                                    >> work/MysqlServer.json
echo "    }"                                                                                     >> work/MysqlServer.json
echo "}"                                                                                         >> work/MysqlServer.json

pv entity create --payloadFile=work/MysqlServer.json > work/temp0
cat work/temp0 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

echo " " >>  ${log_file}
echo "# Mysql Schema creation " >>  ${log_file}

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

    echo "{"                                                                                      > work/MysqlSchema.json
    echo "\"entity\": "                                                                          >> work/MysqlSchema.json
    echo "   { "                                                                                 >> work/MysqlSchema.json
    echo "      \"typeName\": \"mysql_schema\", "                                                >> work/MysqlSchema.json
    echo "      \"attributes\": { "                                                              >> work/MysqlSchema.json
    echo "            \"name\": \"${schema}\", "                                                 >> work/MysqlSchema.json
    echo "            \"qualifiedName\": \"mysql://${source_name}/${schema}\", "                 >> work/MysqlSchema.json
    echo "            \"description\": \"Manual Schema ${schema} for Mysql db\", "               >> work/MysqlSchema.json
    echo "            \"db\": { "                                                                >> work/MysqlSchema.json
    echo "               \"typeName\": \"mysql_server\", "                                       >> work/MysqlSchema.json
    echo "               \"uniqueAttributes\": { "                                               >> work/MysqlSchema.json
    echo "                  \"qualifiedName\": \"mysql://{source_name}\" "                       >> work/MysqlSchema.json
    echo "                 } "                                                                   >> work/MysqlSchema.json
    echo "         },"                                                                           >> work/MysqlSchema.json
    echo "        \"relationshipAttributes\": {"                                                 >> work/MysqlSchema.json
    echo "            \"mysql_server\": {"                                                       >> work/MysqlSchema.json
    echo "            \"qualifiedName\": \"mysql://${source_name}\" "                            >> work/MysqlSchema.json
    echo "            }"                                                                         >> work/MysqlSchema.json
    echo "         } "                                                                           >> work/MysqlSchema.json
    echo "    } "                                                                                >> work/MysqlSchema.json
    echo "  } "                                                                                  >> work/MysqlSchema.json
    echo "} "                                                                                    >> work/MysqlSchema.json

    pv entity create --payloadFile=work/MysqlSchema.json > work/temp1
    cat work/temp1 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}


    echo "Processing relationship - ${source_name}/$schema"

    echo "{   "                                                                                  > work/postgresSchemaRelation.json
    echo "    \"end1\": {  "                                                                    >> work/postgresSchemaRelation.json
    echo "        \"typeName\": \"mysql_server\",  "                                            >> work/postgresSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/postgresSchemaRelation.json
    echo "            \"qualifiedName\": \"mysql://${source_name}\" "                           >> work/MysqlSchemaRelation.json
    echo "        }  "                                                                          >> work/postgresSchemaRelation.json
    echo "    },  "                                                                             >> work/postgresSchemaRelation.json
    echo "    \"end2\": {  "                                                                    >> work/postgresSchemaRelation.json
    echo "        \"typeName\": \"mysql_schema\",  "                                            >> work/postgresSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/postgresSchemaRelation.json
    echo "            \"qualifiedName\": \"mysql://${source_name}/${schema}\" "                 >> work/postgresSchemaRelation.json
    echo "        }  "                                                                          >> work/postgresSchemaRelation.json
    echo "    },  "                                                                             >> work/postgresSchemaRelation.json
    echo "    \"typeName\": \"mysql_server_schemas\"  "                                         >> work/postgresSchemaRelation.json
    echo "}  "                                                                                  >> work/postgresSchemaRelation.json

    pv relationship create --payloadFile=work/MysqlSchemaRelation.json > work/temp2
    cat work/temp2 | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done

echo " " >>  ${log_file}
echo "# Mysql tables creation " >>  ${log_file}

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


    echo "{"                                                                                            > work/MysqlTable.json
    echo "  \"entity\": "                                                                              >> work/MysqlTable.json
    echo "     {"                                                                                      >> work/MysqlTable.json
    echo "      \"kind\": \"mysql_table\", "                                                           >> work/MysqlTable.json
    echo "      \"typeName\": \"mysql_table\", "                                                       >> work/MysqlTable.json
    echo "      \"attributes\": { "                                                                    >> work/MysqlTable.json
    echo "        \"name\": \"${table}\", "                                                            >> work/MysqlTable.json
    echo "        \"qualifiedName\": \"mysql://${source_name}/${schema}/${table}\", "                  >> work/MysqlTable.json
    echo "        \"owner\": null, "                                                                   >> work/MysqlTable.json
    echo "        \"description\": \"Inserted using script\" "                                         >> work/MysqlTable.json
    echo "     },"                                                                                     >> work/MysqlTable.json
    echo "      \"collectionId\": \"${collection_id}\", "                                              >> work/MysqlTable.json
    echo "      \"displayText\": \"${table}\", "                                                       >> work/MysqlTable.json
    echo "      \"domainId\": \"${domain_id}\" "                                                       >> work/MysqlTable.json
    echo "   }"                                                                                        >> work/MysqlTable.json
    echo "}"                                                                                           >> work/MysqlTable.json

    pv entity create --payloadFile=work/MysqlTable.json > work/temp3
    cat work/temp3 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

    echo "Processing relationship - $schema.$table"

    echo "{  "                                                                                           > work/MysqlTableRelation.json
    echo "    \"end1\": { "                                                                             >> work/MysqlTableRelation.json
    echo "        \"typeName\": \"mysql_schema\", "                                                     >> work/MysqlTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                             >> work/MysqlTableRelation.json
    echo "             \"qualifiedName\": \"mysql://${source_name}/${schema}\" "                        >> work/MysqlTableRelation.json
    echo "        } "                                                                                   >> work/MysqlTableRelation.json
    echo "    }, "                                                                                      >> work/MysqlTableRelation.json
    echo "    \"end2\": { "                                                                             >> work/MysqlTableRelation.json
    echo "        \"typeName\": \"mysql_table\", "                                                      >> work/MysqlTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                             >> work/MysqlTableRelation.json
    echo "            \"qualifiedName\": \"mysql://${source_name}/${schema}/${table}\" "                >> work/MysqlTableRelation.json
    echo "        } "                                                                                   >> work/MysqlTableRelation.json
    echo "    }, "                                                                                      >> work/MysqlTableRelation.json
    echo "    \"typeName\": \"mysql_schema_tables\" "                                                   >> work/MysqlTableRelation.json
    echo "} "                                                                                           >> work/MysqlTableRelation.json

    pv relationship create --payloadFile=work/MysqlTableRelation.json > work/temp4
    cat work/temp4  | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done
