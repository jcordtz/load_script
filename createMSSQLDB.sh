if [ "$#" -ne 4 ]
then
  echo "Incorrect number of arguments"
  echo "Usage: $0 <domain_id> <collection_id> <source name> <input_file>"
  exit 1
fi

domain_id=$1
collection_id=$2
source_name=$3
source_db=${3}_db
workspace=$3
catalog=$3
input_file=$4
log_file=work/${3}_$$_log.sh

echo
echo "Creating MSSQL Database Purview entries"
echo
echo "All processing is registered in ** ${log_file} ** with corresponding delete statements"
echo

echo "# MSSQL Server/Database creation - job data `date`" >  ${log_file}
chmod +x ${log_file}

echo
echo "NOTE:"
echo "Statements in this file can be execute one-by-one or the complete file"
echo "can be run to remove all that has been done in this job"
echo

echo " " >> ${log_file}
echo "# NOTE:" >> ${log_file}
echo "# Statements in this file can be execute one-by-one or the complete file" >> ${log_file}
echo "# can be run to remove all that has been done in this job" >> ${log_file}
echo " " >> ${log_file}

echo "Processing Server - ${source_name}"
echo "# Server - ${source_name}" >> ${log_file}

echo "{"                                                                                          > work/MSSQLServer.json
echo "    \"entity\": {"                                                                         >> work/MSSQLServer.json
echo "        \"attributes\": {"                                                                 >> work/MSSQLServer.json
echo "            \"description\": \"Manual MSSQL server named ${source_name}.\","               >> work/MSSQLServer.json
echo "            \"name\": \"${source_db}\","                                                   >> work/MSSQLServer.json
echo "            \"qualifiedName\": \"mssql://${source_name}\""                                 >> work/MSSQLServer.json
echo "        },"                                                                                >> work/MSSQLServer.json
echo "        \"collection\": {"                                                                 >> work/MSSQLServer.json
echo "            \"type\": \"CollectionReference\","                                            >> work/MSSQLServer.json
echo "            \"referenceName\": \"$collection_id\""                                         >> work/MSSQLServer.json
echo "        },"                                                                                >> work/MSSQLServer.json
echo "        \"collectionId\": \"$collection_id\","                                             >> work/MSSQLServer.json
echo "        \"domainId\": \"$domain_id\","                                                     >> work/MSSQLServer.json
echo "        \"typeName\": \"azure_sql_server\""                                                >> work/MSSQLServer.json
echo "    }"                                                                                     >> work/MSSQLServer.json
echo "}"                                                                                         >> work/MSSQLServer.json

pv entity create --payloadFile=work/MSSQLServer.json > work/temp00
cat work/temp00 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

echo "Processing database - ${source_name}/${source_db}"

echo " " >> ${log_file}
echo "# Database - ${source_name}/${source_db}" >> ${log_file}

echo "{"                                                                                          > work/MSSQLDatabase.json
echo "    \"entity\": {"                                                                         >> work/MSSQLDatabase.json
echo "        \"attributes\": {"                                                                 >> work/MSSQLDatabase.json
echo "            \"description\": \"Manual MSSQL database named ${source_db}.\","               >> work/MSSQLDatabase.json
echo "            \"name\": \"${source_db}\","                                                   >> work/MSSQLDatabase.json
echo "            \"qualifiedName\": \"mssql://${source_name}/${source_db}\""                    >> work/MSSQLDatabase.json
echo "        },"                                                                                >> work/MSSQLDatabase.json
echo "        \"collection\": {"                                                                 >> work/MSSQLDatabase.json
echo "            \"type\": \"CollectionReference\","                                            >> work/MSSQLDatabase.json
echo "            \"referenceName\": \"$collection_id\""                                         >> work/MSSQLDatabase.json
echo "        },"                                                                                >> work/MSSQLDatabase.json
echo "        \"collectionId\": \"$collection_id\","                                             >> work/MSSQLDatabase.json
echo "        \"domainId\": \"$domain_id\","                                                     >> work/MSSQLDatabase.json
echo "        \"typeName\": \"azure_sql_db\""                                                    >> work/MSSQLDatabase.json
echo "    }"                                                                                     >> work/MSSQLDatabase.json
echo "}"                                                                                         >> work/MSSQLDatabase.json

pv entity create --payloadFile=work/MSSQLDatabase.json > work/temp01
cat work/temp01 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

echo "Processing Database relationship - ${source_name}/${source_db}"

echo "{   "                                                                                  > work/MSSQLDatabaseRelation.json
echo "    \"end1\": {  "                                                                    >> work/MSSQLDatabaseRelation.json
echo "        \"typeName\": \"azure_sql_db\",  "                                            >> work/MSSQLDatabaseRelation.json
echo "        \"uniqueAttributes\": {  "                                                    >> work/MSSQLDatabaseRelation.json
echo "            \"qualifiedName\": \"mssql://${source_name}/${source_db}\" "              >> work/MSSQLDatabaseRelation.json
echo "        }  "                                                                          >> work/MSSQLDatabaseRelation.json
echo "    },  "                                                                             >> work/MSSQLDatabaseRelation.json
echo "    \"end2\": {  "                                                                    >> work/MSSQLDatabaseRelation.json
echo "        \"typeName\": \"azure_sql_server\",  "                                        >> work/MSSQLDatabaseRelation.json
echo "        \"uniqueAttributes\": {  "                                                    >> work/MSSQLDatabaseRelation.json
echo "            \"qualifiedName\": \"mssql://${source_name}\" "                           >> work/MSSQLDatabaseRelation.json
echo "        }  "                                                                          >> work/MSSQLDatabaseRelation.json
echo "    },  "                                                                             >> work/MSSQLDatabaseRelation.json
echo "    \"typeName\": \"azure_sql_server_databases\"  "                                   >> work/MSSQLDatabaseRelation.json
echo "}  "                                                                                  >> work/MSSQLDatabaseRelation.json

pv relationship create --payloadFile=work/MSSQLDatabaseRelation.json > work/temp02
cat work/temp02 | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

echo " " >>  ${log_file}
echo "# MSSQL Schema creation " >>  ${log_file}
echo " " >> ${log_file}

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

    echo "Processing schema - ${source_name}/${source_db}/${schema}"
    echo "# Schema : ${source_name}/${source_db}/${schema}" >>  ${log_file}

    echo "{"                                                                                      > work/MSSQLSchema.json
    echo "\"entity\": "                                                                          >> work/MSSQLSchema.json
    echo "   { "                                                                                 >> work/MSSQLSchema.json
    echo "      \"typeName\": \"azure_sql_schema\", "                                            >> work/MSSQLSchema.json
    echo "      \"attributes\": { "                                                              >> work/MSSQLSchema.json
    echo "            \"name\": \"${schema}\", "                                                 >> work/MSSQLSchema.json
    echo "            \"qualifiedName\": \"mssql://${source_name}/${source_db}/${schema}\", "    >> work/MSSQLSchema.json
    echo "            \"description\": \"Manual Schema ${schema} for MSSQL db\" "                >> work/MSSQLSchema.json
    echo "         },"                                                                           >> work/MSSQLSchema.json
    echo "        \"collectionId\": \"$collection_id\","                                         >> work/MSSQLSchema.json
    echo "        \"domainId\": \"$domain_id\" "                                                 >> work/MSSQLSchema.json
    echo "    } "                                                                                >> work/MSSQLSchema.json
    echo "} "                                                                                    >> work/MSSQLSchema.json

    pv entity create --payloadFile=work/MSSQLSchema.json > work/temp1
    cat work/temp1 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}


    echo "Processing relationship - ${source_name}/${source_db}/$schema"

    echo "{   "                                                                                  > work/MSSQLSchemaRelation.json
    echo "    \"end1\": {  "                                                                    >> work/MSSQLSchemaRelation.json
    echo "        \"typeName\": \"azure_sql_db\",  "                                            >> work/MSSQLSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/MSSQLSchemaRelation.json
    echo "            \"qualifiedName\": \"mssql://${source_name}/${source_db}\" "              >> work/MSSQLSchemaRelation.json
    echo "        }  "                                                                          >> work/MSSQLSchemaRelation.json
    echo "    },  "                                                                             >> work/MSSQLSchemaRelation.json
    echo "    \"end2\": {  "                                                                    >> work/MSSQLSchemaRelation.json
    echo "        \"typeName\": \"azure_sql_schema\",  "                                        >> work/MSSQLSchemaRelation.json
    echo "        \"uniqueAttributes\": {  "                                                    >> work/MSSQLSchemaRelation.json
    echo "            \"qualifiedName\": \"mssql://${source_name}/${source_db}/${schema}\" "    >> work/MSSQLSchemaRelation.json
    echo "        }  "                                                                          >> work/MSSQLSchemaRelation.json
    echo "    },  "                                                                             >> work/MSSQLSchemaRelation.json
    echo "    \"typeName\": \"azure_sql_db_schemas\"  "                                         >> work/MSSQLSchemaRelation.json
    echo "}  "                                                                                  >> work/MSSQLSchemaRelation.json

    pv relationship create --payloadFile=work/MSSQLSchemaRelation.json > work/temp2
    cat work/temp2 | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done

echo " " >>  ${log_file}
echo "# MSSQL tables creation " >>  ${log_file}
echo " " >> ${log_file}

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


    echo "{"                                                                                            > work/MSSQLTable.json
    echo "  \"entity\": "                                                                              >> work/MSSQLTable.json
    echo "     {"                                                                                      >> work/MSSQLTable.json
    echo "      \"kind\": \"azure_sql_table\", "                                                       >> work/MSSQLTable.json
    echo "      \"typeName\": \"azure_sql_table\", "                                                   >> work/MSSQLTable.json
    echo "      \"attributes\": { "                                                                    >> work/MSSQLTable.json
    echo "        \"name\": \"${table}\", "                                                            >> work/MSSQLTable.json
    echo "        \"qualifiedName\": \"mssql://${source_name}/${source_db}/${schema}/${table}\", "     >> work/MSSQLTable.json
    echo "        \"owner\": null, "                                                                   >> work/MSSQLTable.json
    echo "        \"description\": \"Inserted using script\" "                                         >> work/MSSQLTable.json
    echo "     },"                                                                                     >> work/MSSQLTable.json
    echo "      \"collectionId\": \"${collection_id}\", "                                              >> work/MSSQLTable.json
    echo "      \"displayText\": \"${table}\", "                                                       >> work/MSSQLTable.json
    echo "      \"domainId\": \"${domain_id}\" "                                                       >> work/MSSQLTable.json
    echo "   }"                                                                                        >> work/MSSQLTable.json
    echo "}"                                                                                           >> work/MSSQLTable.json

    pv entity create --payloadFile=work/MSSQLTable.json > work/temp3
    cat work/temp3 | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

    echo "Processing relationship - $schema.$table"

    echo "{  "                                                                                           > work/MSSQLTableRelation.json
    echo "    \"end1\": { "                                                                             >> work/MSSQLTableRelation.json
    echo "        \"typeName\": \"azure_sql_schema\", "                                                 >> work/MSSQLTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                             >> work/MSSQLTableRelation.json
    echo "             \"qualifiedName\": \"mssql://${source_name}/${source_db}/${schema}\" "           >> work/MSSQLTableRelation.json
    echo "        } "                                                                                   >> work/MSSQLTableRelation.json
    echo "    }, "                                                                                      >> work/MSSQLTableRelation.json
    echo "    \"end2\": { "                                                                             >> work/MSSQLTableRelation.json
    echo "        \"typeName\": \"azure_sql_table\", "                                                  >> work/MSSQLTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                             >> work/MSSQLTableRelation.json
    echo "            \"qualifiedName\": \"mssql://${source_name}/${source_db}/${schema}/${table}\" "   >> work/MSSQLTableRelation.json
    echo "        } "                                                                                   >> work/MSSQLTableRelation.json
    echo "    }, "                                                                                      >> work/MSSQLTableRelation.json
    echo "    \"typeName\": \"azure_sql_schema_tables\" "                                               >> work/MSSQLTableRelation.json
    echo "} "                                                                                           >> work/MSSQLTableRelation.json

    pv relationship create --payloadFile=work/MSSQLTableRelation.json > work/temp4
    cat work/temp4  | grep "\"guid\"" | tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done
