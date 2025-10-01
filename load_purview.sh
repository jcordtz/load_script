echo
echo "Placeholder Setup for Purview"
echo "============================="
echo

# creating default values
system_type_def=3
system_name_def=SystemName$RANDOM
owner_def=Owner$RANDOM
table_name_def=table_name$RANDOM
input_file_name_def=tables.csv


echo "This script will create a placeholder, that can be used for referencing"
echo "for a system that can not be scanned due to what ever reason"
echo
echo "The script supports creating systems of type MSSql, Oracle and Databricks"
echo
echo "Getting Purview Account"

if [ "$PURVIEW_NAME" == "" ]; then
   echo "The parameter PURVIEW_NAME must be set"
   echo 
   echo "Have you run \". ./init_purview.sh\""
   echo
   echo "Setup aborted"
   exit -1
fi

if [ "$AZURE_CLIENT_ID" == "" ]; then
   echo "The parameter AZURE_CLIENT_ID must be set"
   echo 
   echo "Have you run \". ./init_purview.sh\""
   echo
   echo "Setup aborted"
   exit -1
fi

if [ "$AZURE_CLIENT_SECRET" == "" ]; then
   echo "The parameter AZURE_CLIENT_SECRET must be set"
   echo 
   echo "Have you run \". ./init_purview.sh\""
   echo
   echo "Setup aborted"
   exit -1
fi


if [ "$AZURE_TENANT_ID" == "" ]; then
   echo "The parameter AZURE_TENANT_ID must be set"
   echo 
   echo "Have you run \". ./init_purview.sh\""
   echo
   echo "Setup aborted"
   exit -1
fi

friendlyName=""
friendlyName=`pv account getAccount | grep friendly | sed -e "s/.*Name.:..//" -e "s/.,//"`


if [ "$friendlyName" == "" ]; then
   echo "The purview account cannot be found"
   echo
   echo "Setup aborted"
   exit -1
fi

parent_collection_def=$PURVIEW_NAME

echo
echo
echo "Using Purview account $PURVIEW_NAME"
echo "with colletion master/friendly name $PURVIEW_NAME/$friendlyName"
echo

read -p "Is this the right Purview account Y/[N] " continue

if [ "$continue" != "Y" ]; then
	echo "Please change the init parameters to reflect the right Purview Account"
	echo
	echo "Setup aborted"
	exit -3
fi

echo
echo "Getting list of collections in Purview account $PURVIEW_NAME"
echo

> collections_list

pv account getCollections |
while read line 
do
     collection_name=`echo $line | grep "friendlyName" | sed -e "s/.*friendlyName.*: \"//" -e "s/.,//"`
     collection_id=`echo $line | grep "name" | sed -e "s/.*name.*: \"//" -e "s/.,//"`

     if [ "$collection_name" != "" ]; then
        save_collection_name=$collection_name
     fi

     if [ "$collection_id" != "" ]; then
	     echo " -- $save_collection_name ($collection_id)" >> collections_list
     fi
done

echo "Enter desired values for the following parameters - default in []:"
echo

echo "Available collections:"
cat collections_list
echo

echo "Name of parent_collection [$parent_collection_def]: " 
read parent_collection
parent_collection=${parent_collection:-$parent_collection_def}

parent_collection_id=`grep "$parent_collection" collections_list | sed -e "s/.*(//" -e "s/)//"`

if [ "$parent_collection_id" == "" ]; then
   echo "The parent collection $parent_collection does not exist"
   echo
   echo "Setup aborted"
   exit -1
fi

echo
echo "Name of System [$system_name_def]: " 
read system_name
system_name=${system_name:-$system_name_def}

# not implemented yet
# echo
# echo "Owner of System $system_name [$owner_def]: " 
# read owner
# owner=${owner:-$owner_def}

echo
echo "Please choose the type of system you want to register $system_name as:"
echo 
echo " 1 - Microsoft SQL (Azure)"
echo " 2 - Oracle Database"
echo " 3 - Databricks"
echo " 4 - PostgreSQL"
echo " 5 - MySQL"
echo
echo "System type of $system_name (1-5) [$system_type_def]: " 
read system_type
system_type=${system_type:-$system_type_def}

case $system_type in
   1) system_type_txt="MSSql"
      echo "Microsoft SQL (Azure) choosen"
      ;;
   2) system_type_txt="Oracle"
      echo "Oracle Database choosen"
      ;;
   3) system_type_txt="Databricks"
      echo "Databricks choosen"
      ;;
   4) system_type_txt="PostgreSQL"
      echo "PostgreSQL choosen"
      ;;
   5) system_type_txt="MySQL"
      echo "MySQL choosen"
      ;;
   *) echo "The system type must be 1 for MSSql, 2 for Oracle,3 for DataBricks, 4 for PostgreSQL or 5 for MySQL"
      echo 
      echo "Setup aborted"
      exit -1
      ;;
esac

echo
echo
echo "Note"
echo
echo "The filename with list of tables must have the format of"
echo "table_schema,table_name,table_type with a header with these names" 
echo

echo "Enter filename with list of tables [$input_file_name_def]: "
read input_file_name
input_file_name=${input_file_name:-$input_file_name_def}

if [ "$input_file_name" == "" ]; then
   echo "The input file name must be set"
   echo 
   echo "Setup aborted"
   exit -1
fi

if ! test -f $input_file_name ; then
   echo "Input file does not exist."
   echo 
   echo "Setup aborted"
   exit -1
fi


echo
echo
echo "Parameters entered"
echo "------------------"
echo "Parent collection: $parent_collection ($parent_collection_id)"
echo "System Name      : $system_name"
echo "System Type      : $system_type for $system_type_txt"
echo "File name        : $input_file_name"
echo

read -p "Do you want to create new assets in Purview with the above parameters Y/[N] " continue

if [ "$continue" != "Y" ]; then
	echo
	echo "Setup aborted"
        echo $NC
	exit -3
fi

echo 

if [ ! -d work ]; then
   mkdir work

   if [ $? -ne 0 ]; then
      echo "Unable to create work directory"
      echo
      echo "Setup aborted"
      exit -3
   fi
fi

if [ $system_type_txt == "MSSql" ]; then
	echo "========================= MSSql =================================="
	echo "Creating Source $system_name in $system_name($parent_collection_id)"
	./createSrcMSSQL.sh $system_name $parent_collection_id
	
	echo "Creating Database ${system_name}_db in $parent_collection_id"
	# Create Database
	./createMSSQLDB.sh $PURVIEW_NAME $parent_collection_id $system_name $input_file_name
fi

if [ $system_type_txt == "Oracle" ]; then
	echo "========================== Oracle ================================"
	echo "Creating Source $system_name in $system_name($parent_collection_id)"
	./createSrcOracle.sh $system_name $parent_collection_id

	echo "Creating Database ${system_name}_db in $parent_collection_id"
	# Create Database
	./createOracleDB.sh $PURVIEW_NAME $parent_collection_id $system_name $input_file_name

fi

if [ $system_type_txt == "Databricks" ]; then
	echo "========================== DataBricks ================================"
	echo "Creating Source $system_name in $system_name($parent_collection_id)"
	./createSrcDataBricks.sh $system_name $parent_collection_id

	echo "Creating Databricks ${system_name}_db in $parent_collection_id"
	# Create Database
	./createDataBricks.sh $PURVIEW_NAME $parent_collection_id $system_name $input_file_name

fi

if [ $system_type_txt == "Postgres" ]; then
	echo "========================== Custom ================================"
	echo "Creating Source $system_name in $system_name($parent_collection_id)"
	./createSrcPostgres.sh $system_name $parent_collection_id

	echo "Creating Database ${system_name}_db in $parent_collection_id"
	# Create Database
	./createPostgresDB.sh $PURVIEW_NAME $parent_collection_id $system_name $input_file_name

fi

if [ $system_type_txt == "MySQL" ]; then
	echo "========================== Custom ================================"
	echo "Creating Source $system_name in $system_name($parent_collection_id)"
	./createSrcMYSQL.sh $system_name $parent_collection_id

	echo "Creating Database ${system_name}_db in $parent_collection_id"
	# Create Database
	./createMYSQLDB.sh $PURVIEW_NAME $parent_collection_id $system_name $input_file_name

fi

if [ $system_type_txt == "Custom" ]; then
	echo "========================== Custom ================================"
	echo "Creating Source $system_name in $system_name($parent_collection_id)"
	# ./createSrcCustom.sh $system_name $parent_collection_id

	echo "Creating Database ${system_name}_db in $parent_collection_id"
	# Create Database
	# ./createCustom.sh $PURVIEW_NAME $parent_collection_id $system_name $input_file_name

fi
