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
echo "Creating Postgres Database Purview entries"
echo
echo "All processing is registered in ** ${log_file} ** with corresponding delete statements"
echo
echo
echo "NOTE:"
echo "Statements in this file can be execute one-by-one or the complete file"
echo "can be run to remove all that has been done in this job"
echo

echo "# Postgres Server/Database creation - job data `date`" >  ${log_file}
chmod +x ${log_file}

echo " " >> ${log_file}
echo "# NOTE:" >> ${log_file}
echo "# Statements in this file can be execute one-by-one or the complete file" >> ${log_file}
echo "# can be run to remove all that has been done in this job" >> ${log_file}
echo " " >> ${log_file}

echo "Processing server - ${source_name}"
echo "# Server - ${source_name}" >> ${log_file}

echo "{"                                                                                          > work/PostgresServer.json
echo "    \"entity\": {"                                                                         >> work/PostgresServer.json
echo "        \"attributes\": {"                                                                 >> work/PostgresServer.json
echo "            \"description\": \"Manual Postgres server named ${source_nane}.\","            >> work/PostgresServer.json
echo "            \"name\": \"${source_nane}\","                                                 >> work/PostgresServer.json
echo "            \"qualifiedName\": \"postgresql://${source_name}\""                            >> work/PostgresServer.json
echo "        },"                                                                                >> work/PostgresServer.json
echo "        \"collection\": {"                                                                 >> work/PostgresServer.json
echo "            \"type\": \"CollectionReference\","                                            >> work/PostgresServer.json
echo "            \"referenceName\": \"$collection_id\""                                         >> work/PostgresServer.json
echo "        },"                                                                                >> work/PostgresServer.json
echo "        \"collectionId\": \"$collection_id\","                                             >> work/PostgresServer.json
echo "        \"domainId\": \"$domain_id\","                                                     >> work/PostgresServer.json
echo "        \"typeName\": \"postgresql_server\""                                               >> work/PostgresServer.json
echo "    }"                                                                                     >> work/PostgresServer.json
echo "}"                                                                                         >> work/PostgresServer.json

pv entity create --payloadFile=work/PostgresServer.json > work/temp0
cat work/temp0 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

echo " " >>  ${log_file}
echo "# Postgres Schema creation " >>  ${log_file}

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

    echo "{"                                                                                      > work/PostgresSchema.json
    echo "\"entity\": "                                                                          >> work/PostgresSchema.json
    echo "   { "                                                                                 >> work/PostgresSchema.json
    echo "      \"typeName\": \"postgresql_schema\", "                                           >> work/PostgresSchema.json
    echo "      \"attributes\": { "                                                              >> work/PostgresSchema.json
    echo "            \"name\": \"${schema}\", "                                                 >> work/PostgresSchema.json
    echo "            \"qualifiedName\": \"postgresql://${source_name}/${schema}\", "            >> work/PostgresSchema.json
    echo "            \"description\": \"Manual Schema ${schema} for Postgres db\", "            >> work/PostgresSchema.json
    echo "            \"db\": { "                                                                >> work/PostgresSchema.json
    echo "               \"typeName\": \"postgresql_server\", "                                  >> work/PostgresSchema.json
    echo "               \"uniqueAttributes\": { "                                               >> work/PostgresSchema.json
    echo "                  \"qualifiedName\": \"postgresql://{source_name}\" "                  >> work/PostgresSchema.json
    echo "                 } "                                                                   >> work/PostgresSchema.json
    echo "         },"                                                                           >> work/PostgresSchema.json
    echo "        \"relationshipAttributes\": {"                                                 >> work/PostgresSchema.json
    echo "            \"postgresql_server\": {"                                                  >> work/PostgresSchema.json
    echo "            \"qualifiedName\": \"postgresql://${source_name}\" "                       >> work/PostgresSchema.json
    echo "            }"                                                                         >> work/PostgresSchema.json
    echo "         } "                                                                           >> work/PostgresSchema.json
    echo "    } "                                                                                >> work/PostgresSchema.json
    echo "  } "                                                                                  >> work/PostgresSchema.json
    echo "} "                                                                                    >> work/PostgresSchema.json

    pv entity create --payloadFile=work/PostgresSchema.json > work/temp1
    cat work/temp1 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}


    echo "Processing relationship - ${source_name}/$schema"

    echo "{   "                                                                                  > work/postgresSchemaRelation.json
    echo "    \"end1\": {  "                                                                    >> work/postgresSchemaRelation.json
    echo "        \"typeName\": \"postgresql_server\",  "                                       >> work/postgresSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/postgresSchemaRelation.json
    echo "            \"qualifiedName\": \"postgresql://${source_name}\" "                      >> work/PostgresSchemaRelation.json
    echo "        }  "                                                                          >> work/postgresSchemaRelation.json
    echo "    },  "                                                                             >> work/postgresSchemaRelation.json
    echo "    \"end2\": {  "                                                                    >> work/postgresSchemaRelation.json
    echo "        \"typeName\": \"postgresql_schema\",  "                                       >> work/postgresSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/postgresSchemaRelation.json
    echo "            \"qualifiedName\": \"postgresql://${source_name}/${schema}\" "            >> work/postgresSchemaRelation.json
    echo "        }  "                                                                          >> work/postgresSchemaRelation.json
    echo "    },  "                                                                             >> work/postgresSchemaRelation.json
    echo "    \"typeName\": \"postgresql_server_schemas\"  "                                    >> work/postgresSchemaRelation.json
    echo "}  "                                                                                  >> work/postgresSchemaRelation.json

    pv relationship create --payloadFile=work/PostgresSchemaRelation.json > work/temp2
    cat work/temp2 | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done

echo " " >>  ${log_file}
echo "# Postgres tables creation " >>  ${log_file}

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


    echo "{"                                                                                            > work/PostgresTable.json
    echo "  \"entity\": "                                                                              >> work/PostgresTable.json
    echo "     {"                                                                                      >> work/PostgresTable.json
    echo "      \"kind\": \"postgresql_table\", "                                                      >> work/PostgresTable.json
    echo "      \"typeName\": \"postgresql_table\", "                                                  >> work/PostgresTable.json
    echo "      \"attributes\": { "                                                                    >> work/PostgresTable.json
    echo "        \"name\": \"${table}\", "                                                            >> work/PostgresTable.json
    echo "        \"qualifiedName\": \"postgresql://${source_name}/${schema}/${table}\", "             >> work/PostgresTable.json
    echo "        \"owner\": null, "                                                                   >> work/PostgresTable.json
    echo "        \"description\": \"Inserted using script\" "                                         >> work/PostgresTable.json
    echo "     },"                                                                                     >> work/PostgresTable.json
    echo "      \"collectionId\": \"${collection_id}\", "                                              >> work/PostgresTable.json
    echo "      \"displayText\": \"${table}\", "                                                       >> work/PostgresTable.json
    echo "      \"domainId\": \"${domain_id}\" "                                                       >> work/PostgresTable.json
    echo "   }"                                                                                        >> work/PostgresTable.json
    echo "}"                                                                                           >> work/PostgresTable.json

    pv entity create --payloadFile=work/PostgresTable.json > work/temp3
    cat work/temp3 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

    echo "Processing relationship - $schema.$table"

    echo "{  "                                                                                          > work/PostgresTableRelation.json
    echo "    \"end1\": { "                                                                            >> work/PostgresTableRelation.json
    echo "        \"typeName\": \"postgresql_schema\", "                                               >> work/PostgresTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                            >> work/PostgresTableRelation.json
    echo "             \"qualifiedName\": \"postgresql://${source_name}/${schema}\" "                  >> work/PostgresTableRelation.json
    echo "        } "                                                                                  >> work/PostgresTableRelation.json
    echo "    }, "                                                                                     >> work/PostgresTableRelation.json
    echo "    \"end2\": { "                                                                            >> work/PostgresTableRelation.json
    echo "        \"typeName\": \"postgresql_table\", "                                                >> work/PostgresTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                            >> work/PostgresTableRelation.json
    echo "            \"qualifiedName\": \"postgresql://${source_name}/${schema}/${table}\" "          >> work/PostgresTableRelation.json
    echo "        } "                                                                                  >> work/PostgresTableRelation.json
    echo "    }, "                                                                                     >> work/PostgresTableRelation.json
    echo "    \"typeName\": \"postgresql_schema_tables\" "                                             >> work/PostgresTableRelation.json
    echo "} "                                                                                          >> work/PostgresTableRelation.json

    pv relationship create --payloadFile=work/PostgresTableRelation.json > work/temp4
    cat work/temp4  | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done
