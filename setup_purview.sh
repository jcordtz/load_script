# Links
# https://github.com/wjohnson/pyapacheatlas
# https://github.com/tayganr/purviewcli
# https://azure.github.io/azure-sdk/releases/latest/all/python.html
# https://www.taygan.co/blog/2020/12/16/azure-purview
# https://www.rakirahman.me/purview-api-with-synapse/#pipeline-walkthrough
# https://docs.microsoft.com/en-us/rest/api/purview/catalogdataplane/discovery/query

echo
echo
echo "Setting up environment variables"
echo
echo "The following required variables needs to be set"
echo
echo " PURVIEW_NAME"
echo " CLIENT_ID"
echo " TENANT_ID"
echo " CLIENT_SECRET"
echo
echo "These parameters must be entered into the \"parameters.txt\""
echo "file before running this script !!!"
echo
echo "Run this script using \"$0\" these variable are set"
echo "in the \"init_purview.sh\" file"
echo
echo "This script only need to be run when changes are made to the variables"
echo
echo "Afterwards the variables can be instancetitate running the command \". .\/init_purview.sh\""
echo
echo "NOTE - if this script is run using the command \"$0 setup\" "
echo "required libaries are installed"
echo
echo
read -p "Do you want to proceed Y/[N] " continue

if [ "$continue" != "Y" ]; then
        echo
        echo "Please change the init parameters to reflect the Purview Setup"
        echo
        echo "Setup aborted"
        exit 1
fi

if [ "$1" == "setup" ]; then

   echo
   echo "Required libraries will be installed"
   echo
   echo "Azure libraries   : azure-cli azure-core azure-identity"
   echo "Purview libraries : azure-purview-account azure-purview-catalog"
   echo "                    azure-purview-scanning azure-purview-administration"
   echo "                    pyapacheatlas purviewcli"
   echo "Python libraries  : pandas"
   echo
   
   read -p "Do you want to install these libaries Y/[N] " continue

   if [ "$continue" != "Y" ]; then
        echo
        echo "Setup aborted"
        exit 2
   fi

   echo
   echo "Installing required libraries"
   echo

   # OS updates
   apt-get update
   apt-get upgrade

   # Azure
   apt install azure-cli
   pip install azure-core
   pip install azure-identity

   # Purview
   pip install azure-purview-account
   pip install azure-purview-catalog
   pip install azure-purview-scanning
   pip install azure-purview-administration
   pip install pyapacheatlas
   pip install purviewcli
   
   # extras
   pip install pandas
   
fi

echo
echo "Setting environment variables and generating the \"init_purview.sh\" file"
echo

# Service principal variables and name of the purview account to be used
export PURVIEW_NAME=`grep PURVIEW_NAME parameters.txt | sed -e "s/PURVIEW_NAME=//"`

export TENANT_ID=`grep TENANT_ID parameters.txt | sed -e "s/TENANT_ID=//"`
export CLIENT_ID=`grep CLIENT_ID parameters.txt | sed -e "s/CLIENT_ID=//"`
export CLIENT_SECRET=`grep CLIENT_SECRET parameters.txt | sed -e "s/CLIENT_SECRET=//"`


echo 
echo "-----------------------------------------------"
echo "Parameteres will be set to the following values"
echo "-----------------------------------------------"
echo "PURVIEW_NAME=${PURVIEW_NAME}"
echo "AZURE_CLIENT_ID/CLIENT_ID=${CLIENT_ID}"
echo "AZURE_TENANT_ID/TENANT_ID=${TENANT_ID}"
echo "AZURE_CLIENT_SECRET/CLIENT_SECRET=${CLIENT_SECRET}"
echo 

read -p "Do you want to proceed Y/[N] " continue

if [ "$continue" != "Y" ]; then
        echo
        echo "Please change the init parameters to reflect the Purview Setup"
        echo
        echo "Setup aborted"
        exit 3
fi


# The same set of Service principal variables with AZURE_ as prefix. 
export AZURE_TENANT_ID=${TENANT_ID}
export AZURE_CLIENT_ID=${CLIENT_ID}
export AZURE_CLIENT_SECRET=${CLIENT_SECRET}

# Service principal variables and name of the purview account to be used

echo > init_purview.sh

if [ ! -f init_purview.sh ]; then
   echo
   echo "Unable to create init_purview.sh file"
   echo
   echo "Setup aborted"
   exit 4
fi

echo "# Service principal variables and name of the purview account to be used" >> init_purview.sh
echo "export PURVIEW_NAME=${PURVIEW_NAME}" >> init_purview.sh

echo >> init_purview.sh

echo "export TENANT_ID=${TENANT_ID}" >> init_purview.sh
echo "export CLIENT_ID=7${CLIENT_ID}" >> init_purview.sh
echo "export CLIENT_SECRET=${CLIENT_SECRET}" >> init_purview.sh

echo >> init_purview.sh

# The same set of Service principal variables with AZURE_ as prefix.

echo "# The same set of Service principal variables with AZURE_ as prefix." >> init_purview.sh
echo "export AZURE_TENANT_ID=${TENANT_ID}" >> init_purview.sh
echo "export AZURE_CLIENT_ID=${CLIENT_ID}" >> init_purview.sh
echo "export AZURE_CLIENT_SECRET=${CLIENT_SECRET}" >> init_purview.sh

# Printing of the setup
#
echo "echo " >> init_purview.sh
echo "echo -------------------------------------------" >> init_purview.sh
echo "echo Parameteres are set to the following values" >> init_purview.sh
echo "echo -------------------------------------------" >> init_purview.sh
echo "echo PURVIEW_NAME=${PURVIEW_NAME}" >> init_purview.sh
echo "echo AZURE_CLIENT_ID=${CLIENT_ID}" >> init_purview.sh
echo "echo AZURE_TENANT_ID=${TENANT_ID}" >> init_purview.sh
echo "echo AZURE_CLIENT_SECRET=${CLIENT_SECRET}" >> init_purview.sh
echo "echo " >> init_purview.sh

echo
echo "init_purview.sh file has been created"
echo
