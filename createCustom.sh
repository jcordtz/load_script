if [ "$#" -ne 4 ]
then
  echo "Incorrect number of arguments"
  echo "Usage: $0 <domain_id> work/<collection_id> <source name> <input_file>"
  exit 1
fi

domain_id=$1
collection_id=$2
source_name=$3
workspace=$3
catalog=$3
input_file=$4
first=1

# register a catalog

echo "Processing catalog - $workspace"

echo "{"                                                                    > work/customCatalog.json
echo "\"entity\": "                                                        >> work/customCatalog.json
echo "   { "                                                               >> work/customCatalog.json
echo "      \"typeName\": \"custom\", "                                    >> work/customCatalog.json
echo "      \"attributes\": { "                                            >> work/customCatalog.json
echo "            \"name\": \"${workspace}\", "                            >> work/customCatalog.json
echo "            \"qualifiedName\": \"${workspace}\", "                   >> work/customCatalog.json
echo "            \"description\": \"${workspace} for Databricks\", "      >> work/customCatalog.json
echo "            \"collection\": {"                                       >> work/customCatalog.json
echo "            \"typeName\": \"collection\", "                          >> work/customCatalog.json
echo "            \"uniqueAttributes\": { "                                >> work/customCatalog.json
echo "                  \"qualifiedName\": \"${collection_id}\" "          >> work/customCatalog.json
echo "            } "                                                      >> work/customCatalog.json
echo "         } "                                                         >> work/customCatalog.json
echo "         }, "                                                        >> work/customCatalog.json
echo "      \"guid\": \"-101\" "                                           >> work/customCatalog.json
echo "    } "                                                              >> work/customCatalog.json
echo "} "                                                                  >> work/customCatalog.json

pv entity create --payloadFile=work/customCatalog.json


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

    echo "Processing schema - ${workspace}.${schema}"

    echo "{"                                                                    > work/customSchema.json
    echo "\"entity\": "                                                        >> work/customSchema.json
    echo "   { "                                                               >> work/customSchema.json
    echo "      \"typeName\": \"custom_schema\", "                             >> work/customSchema.json
    echo "      \"attributes\": { "                                            >> work/customSchema.json
    echo "            \"name\": \"${schema}\", "                               >> work/customSchema.json
    echo "            \"qualifiedName\": \"${workspace}.${schema}\", "         >> work/customSchema.json
    echo "            \"description\": \"${schema} for Databricks\", "         >> work/customSchema.json
    echo "            \"catalog\": {"                                          >> work/customSchema.json
    echo "            \"typeName\": \"custom_catalog\", "                      >> work/customSchema.json
    echo "            \"uniqueAttributes\": { "                                >> work/customSchema.json
    echo "                  \"qualifiedName\": \"${workspace}\" "              >> work/customSchema.json
    echo "            } "                                                      >> work/customSchema.json
    echo "         } "                                                         >> work/customSchema.json
    echo "         }, "                                                        >> work/customSchema.json
    echo "      \"guid\": \"-102\" "                                           >> work/customSchema.json
    echo "    } "                                                              >> work/customSchema.json
    echo "} "                                                                  >> work/customSchema.json

    pv entity create --payloadFile=work/customSchema.json

done

# find and load tables 

echo "{" > work/customTables.json
echo "  \"entities\": [" >> work/customTables.json

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

    if [ $first -eq 1 ]
    then
        echo "    {" >> work/customTables.json
        first=0
    else
        echo "    }," >> work/customTables.json
        echo "    {"  >> work/customTables.json
    fi


    echo "      \"typeName\": \"custom_table\", "                              >> work/customTables.json
    echo "      \"attributes\": { "                                            >> work/customTables.json
    echo "        \"qualifiedName\": \"${workspace}.${schema}.${table}\", "    >> work/customTables.json
    echo "        \"name\": \"${table}\", "                                    >> work/customTables.json
    echo "        \"owner\": null, "                                           >> work/customTables.json
    echo "        \"description\": \"Inserted using script\" "                 >> work/customTables.json
    echo "        }, "                                                         >> work/customTables.json
    echo "      \"collectionId\": \"${collection_id}\", "                      >> work/customTables.json
    echo "      \"displayText\": \"${table}\", "                               >> work/customTables.json
    echo "      \"domainId\": \"${domain_id}\" "                               >> work/customTables.json

done

echo "   }" >> work/customTables.json
echo " ]"   >> work/customTables.json
echo "}"    >> work/customTables.json

pv entity createBulk --payloadFile=work/customTables.json

cat $input_file |
while read line
do

    schema=`echo $line | cut -d"," -f1`
    table=`echo $line | cut -d"," -f2`

    if [ "$schema" == "table_schema" ]
    then
       continue
    fi

    echo "Processing relationship - $schema.$table"

    echo "{  "                                                                    > work/customRelation.json
    echo "    \"end1\": { "                                                      >> work/customRelation.json
    echo "        \"typeName\": \"custom_schema\", "                             >> work/customRelation.json
    echo "        \"uniqueAttributes\": { "                                      >> work/customRelation.json
    echo "             \"qualifiedName\": \"${workspace}.${schema}\" "           >> work/customRelation.json
    echo "        } "                                                            >> work/customRelation.json
    echo "    }, "                                                               >> work/customRelation.json
    echo "    \"end2\": { "                                                      >> work/customRelation.json
    echo "        \"typeName\": \"custom_table\", "                              >> work/customRelation.json
    echo "        \"uniqueAttributes\": { "                                      >> work/customRelation.json
    echo "             \"qualifiedName\": \"${workspace}.${schema}.${table}\" "  >> work/customRelation.json
    echo "        } "                                                            >> work/customRelation.json
    echo "    }, "                                                               >> work/customRelation.json
    echo "    \"typeName\": \"custom_schema_tables\" "                           >> work/customRelation.json
    echo "} "                                                                    >> work/customRelation.json

    pv relationship create --payloadFile=work/customRelation.json

done
