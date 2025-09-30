if [ "$#" -ne 4 ]
then
  echo "Incorrect number of arguments"
  echo "Usage: $0 <domain_id> <collection_id> <source name> <input_file>"
  exit 1
fi

domain_id=$1
collection_id=$2
source_name=$3
workspace=$3
catalog=$3
input_file=$4
log_file=work/${3}_$$_log.sh

echo
echo "Creating Databricks Purview entries"
echo
echo "All processing is registered in ** ${log_file} ** with corresponding delete statements"
echo
echo "NOTE:"
echo "Statements in this file can be execute one-by-one or the complete file"
echo "can be run to remove all that has been done in this job"
echo

echo "# Databricks - job date `date`" >  ${log_file}
chmod +x ${log_file}

echo " " >> ${log_file}
echo "# NOTE:" >> ${log_file}
echo "# Statements in this file can be execute one-by-one or the complete file" >> ${log_file}
echo "# can be run to remove all that has been done in this job" >> ${log_file}
echo " " >> ${log_file}


echo "Processing catalog - $workspace"
echo "# Processing catalog ${workpace}" >> ${log_file}

# register a catalog


echo "{"                                                                    > work/DatabricksCatalog.json
echo "\"entity\": "                                                        >> work/DatabricksCatalog.json
echo "   { "                                                               >> work/DatabricksCatalog.json
echo "      \"typeName\": \"databricks_catalog\", "                        >> work/DatabricksCatalog.json
echo "      \"attributes\": { "                                            >> work/DatabricksCatalog.json
echo "            \"name\": \"${workspace}\", "                            >> work/DatabricksCatalog.json
echo "            \"qualifiedName\": \"Databricks://${workspace}\", "      >> work/DatabricksCatalog.json
echo "            \"description\": \"${workspace} for Databricks\" "       >> work/DatabricksCatalog.json
echo "            },"                                                      >> work/DatabricksCatalog.json
echo "      \"relationshipAttributes\": { "                                >> work/DatabricksCatalog.json
echo "            \"collection\": {"                                       >> work/DatabricksCatalog.json
echo "                  \"qualifiedName\": \"${collection_id}\" "          >> work/DatabricksCatalog.json
echo "            } "                                                      >> work/DatabricksCatalog.json
echo "         } "                                                         >> work/DatabricksCatalog.json
echo "    } "                                                              >> work/DatabricksCatalog.json
echo "} "                                                                  >> work/DatabricksCatalog.json

pv entity create --payloadFile=work/DatabricksCatalog.json > work/temp0
cat work/temp0  | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

# find and load schemas 

echo " " >>  ${log_file}
echo "# Databricks schema creation " >>  ${log_file}

cat $input_file | 
cut -d"," -f1 |
sort -u |
while read schema
do

    if [ "$schema" == "table_schema" ]
    then
       continue
    fi

    echo "Processing schema - ${workspace}.${schema}"
    echo "# Schema : ${workspace}.${schema}" >>  ${log_file}


    echo "{"                                                                          > work/DatabricksSchema.json
    echo "\"entity\": "                                                              >> work/DatabricksSchema.json
    echo "   { "                                                                     >> work/DatabricksSchema.json
    echo "      \"typeName\": \"databricks_schema\", "                               >> work/databricksSchema.json
    echo "      \"attributes\": { "                                                  >> work/DatabricksSchema.json
    echo "            \"name\": \"${schema}\", "                                     >> work/DatabricksSchema.json
    echo "            \"qualifiedName\": \"Databricks://${workspace}.${schema}\", "  >> work/databricksSchema.json
    echo "            \"description\": \"${schema} for Databricks\" "                >> work/DatabricksSchema.json
    echo "            },"                                                            >> work/DatabricksSchema.json
    echo "      \"relationshipAttributes\": { "                                      >> work/DatabricksSchema.json
    echo "            \"Databricks_catalog\": {"                                     >> work/databricksSchema.json
    echo "                  \"qualifiedName\": \"Databricks://${workspace}\" "       >> work/databricksSchema.json
    echo "            } "                                                            >> work/DatabricksSchema.json
    echo "         } "                                                               >> work/DatabricksSchema.json
    echo "    } "                                                                    >> work/DatabricksSchema.json
    echo "} "                                                                        >> work/DatabricksSchema.json

    pv entity create --payloadFile=work/DatabricksSchema.json > work/temp1
    cat work/temp1  | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

    echo "Processing relationship - $workspace.$schema"
    echo "# Relationship : ${workspace}.${schema}" >>  ${log_file}

    echo "{  "                                                                         > work/DatabricksSchemaRelation.json
    echo "    \"end1\": { "                                                           >> work/DatabricksSchemaRelation.json
    echo "        \"typeName\": \"databricks_catalog\", "                             >> work/databricksSchemaRelation.json
    echo "        \"uniqueAttributes\": { "                                           >> work/DatabricksSchemaRelation.json
    echo "             \"qualifiedName\": \"Databricks://${workspace}\" "             >> work/databricksSchemaRelation.json
    echo "        } "                                                                 >> work/DatabricksSchemaRelation.json
    echo "    }, "                                                                    >> work/DatabricksSchemaRelation.json
    echo "    \"end2\": { "                                                           >> work/DatabricksSchemaRelation.json
    echo "        \"typeName\": \"databricks_schema\", "                              >> work/databricksSchemaRelation.json
    echo "        \"uniqueAttributes\": { "                                           >> work/DatabricksSchemaRelation.json
    echo "             \"qualifiedName\": \"Databricks://${workspace}.${schema}\" "   >> work/databricksSchemaRelation.json
    echo "        } "                                                                 >> work/DatabricksSchemaRelation.json
    echo "    }, "                                                                    >> work/DatabricksSchemaRelation.json
    echo "    \"typeName\": \"databricks_catalog_schemas\" "                          >> work/databricksSchemaRelation.json
    echo "} "                                                                         >> work/DatabricksSchemaRelation.json

    pv relationship create --payloadFile=work/DatabricksSchemaRelation.json > work/temp2
    cat work/temp2  | grep "\"guid\""| tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done

# find and load tables 

echo " " >>  ${log_file}
echo "# Databricks table creation " >>  ${log_file}

cat $input_file |
while read line
do

    schema=`echo $line | cut -d"," -f1`
    table=`echo $line | cut -d"," -f2`

    if [ "$schema" == "table_schema" ]
    then
       continue
    fi

    echo "Processing table - ${schema}.${table}"
    echo "# table : ${schema}.${table}" >>  ${log_file}


    echo "{"                                                                                 > work/DatabricksTable.json
    echo "  \"entity\": "                                                                   >> work/DatabricksTable.json
    echo "   { "                                                                            >> work/DatabricksTable.json
    echo "      \"typeName\": \"databricks_table\", "                                       >> work/databricksTable.json
    echo "      \"attributes\": { "                                                         >> work/DatabricksTable.json
    echo "        \"qualifiedName\": \"Databricks://${workspace}.${schema}.${table}\", "    >> work/databricksTable.json
    echo "        \"name\": \"${table}\", "                                                 >> work/DatabricksTable.json
    echo "        \"description\": \"Inserted using script\" "                              >> work/DatabricksTable.json
    echo "        }, "                                                                      >> work/DatabricksTable.json
    echo "      \"relationshipAttributes\": { "                                             >> work/DatabricksTable.json
    echo "            \"Databricks_schema\": {"                                             >> work/databricksTable.json
    echo "                  \"qualifiedName\": \"Databricks://${workspace}.${schema}\" "    >> work/databricksTable.json
    echo "            } "                                                                   >> work/DatabricksTable.json
    echo "         } "                                                                      >> work/DatabricksTable.json
    echo "   }"                                                                             >> work/DatabricksTable.json
    echo "} "                                                                               >> work/DatabricksTable.json

    pv entity create --payloadFile=work/DatabricksTable.json > work/temp3
    cat work/temp3  | grep "\"guid\"" | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv entity delete --guid=\1/" >> ${log_file}

    echo "Processing relationship - ${schema}.${table}"
    echo "# relationship : ${schema}.${table}" >>  ${log_file}

    echo "{  "                                                                                 > work/DatabricksTableRelation.json
    echo "    \"end1\": { "                                                                   >> work/DatabricksTableRelation.json
    echo "        \"typeName\": \"databricks_schema\", "                                      >> work/databricksTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                   >> work/DatabricksTableRelation.json
    echo "             \"qualifiedName\": \"Databricks://${workspace}.${schema}\" "           >> work/databricksTableRelation.json
    echo "        } "                                                                         >> work/DatabricksTableRelation.json
    echo "    }, "                                                                            >> work/DatabricksTableRelation.json
    echo "    \"end2\": { "                                                                   >> work/DatabricksTableRelation.json
    echo "        \"typeName\": \"databricks_table\", "                                       >> work/databricksTableRelation.json
    echo "        \"uniqueAttributes\": { "                                                   >> work/DatabricksTableRelation.json
    echo "             \"qualifiedName\": \"Databricks://${workspace}.${schema}.${table}\" "  >> work/databricksTableRelation.json
    echo "        } "                                                                         >> work/DatabricksTableRelation.json
    echo "    }, "                                                                            >> work/DatabricksTableRelation.json
    echo "    \"typeName\": \"databricks_schema_tables\" "                                    >> work/databricksTableRelation.json
    echo "} "                                                                                 >> work/DatabricksTableRelation.json

    pv relationship create --payloadFile=work/DatabricksTableRelation.json > work/temp4
    cat work/temp4  | grep "\"guid\""| tail -1 | sed -e "s/.*\"guid\": \"\(.*\)\",*/pv relationship delete --guid=\1/" >> ${log_file}

done
