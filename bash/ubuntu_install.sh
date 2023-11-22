#!/bin/bash

# A script to automate an Azure Ubuntu Server deploy for testings

CHECKOUT_PATH=$HOME/code/clim-recal
ANACONDA_INSTALL_FOLDER=$HOME/code/anaconda-install
ANACONDA_INSTALL_SCRIPT_FILE_NAME=Anaconda3-2023.07-2-Linux-x86_64.sh
ANACONDA_INSTALL_URL=https://repo.anaconda.com/archive/$ANACONDA_INSTALL_SCRIPT_FILE_NAME
VMFILESHARE_PATH=/mnt/vmfileshare
AZURE_STORAGE_NAME=dymestorage1

sudo apt-get update && sudo apt-get -y install locales gdal-bin python3-gdal libgdal-dev build-essential wget && sudo apt-get upgrade

cd $CHECKOUT_PATH/python/debiasing && git submodule update --init --recursive

function set_gb_locale {
    sudo echo "en_GB.UTF-8 UTF-8" > /etc/locale.gen && locale-genmake install
}

function install_anaconda {
    mkdir -p $ANACONDA_INSTALL_PATH
    cd $ANACONDA_INSTALL_PATH
    wget $ANACONDA_INSTALL_URL
    bash $ANACONDA_INSTALL_SCRIPT_FILE_NAME
}

function set_azure_credentials {
    echo adding $AZURE_STORAGE_NAME credentials via password provided
    if [ -f /etc/smbcredentials/${AZURE_STORAGE_NAME}.cred ]; then
	echo Replaceing /etc/smbcredentials/${AZURE_STORAGE_NAME}.cred
	sudo rm /etc/smbcredentials/${AZURE_STORAGE_NAME}.cred
    fi
    sudo bash -c 'echo "username='${AZURE_STORAGE_NAME}'" >> /etc/smbcredentials/'${AZURE_STORAGE_NAME}'.cred'
    sudo bash -c 'echo "password='${PASSWORD}'" >> /etc/smbcredentials/'${AZURE_STORAGE_NAME}'.cred'
    sudo chmod 600 /etc/smbcredentials/$AZURE_STORAGE_NAME.cred
}

function mount_vmfileshare {
    echo $VMFILESHARE_PATH is needed to run default model configurations
    echo

    while true; do
        read -p "Would you like to mount vmfileshare to $VMFILESHARE_PATH (needed for running models)? " yn
        case $yn in
    	[Yy]* ) echo Please make sure you have an acess key for $AZURE_STORAGE_NAME ; break;;
    	[Nn]* ) exit;;
    	* ) echo "Please answer yes or no.";;
        esac
    done

    if [ ! -d $VMFILESHARE_PATH ]; then
        sudo mkdir $VMFILESHARE_PATH
    fi

    read -s -p "Access key for $AZURE_STORAGE_NAME: " PASSWORD
    echo

    if [ ! -d "/etc/smbcredentials" ]; then
	echo Createing /etc/smbcredentials
        sudo mkdir /etc/smbcredentials
    fi

    if [ -f "/etc/smbcredentials/${AZURE_STORAGE_NAME}.cred" ]; then
        while true; do
            read -p "Would you like to reset ${AZURE_STORAGE_NAME} credentials? " yn
            case $yn in
        	[Yy]* ) set_azure_credentials ; break;;
        	[Nn]* ) break;;
        	* ) echo "Please answer yes or no.";;
            esac
        done
    else
        set_azure_credentials
    fi

    echo Mounting $AZURE_STORAGE_NAME to $VMFILESHARE_PATH

    sudo bash -c 'echo "//'${AZURE_STORAGE_NAME}'.file.core.windows.net/vmfileshare '${VMFILESHARE_PATH}' cifs nofail,credentials=/etc/smbcredentials/'${AZURE_STORAGE_NAME}'.cred,dir_mode=0777,file_mode=0777,serverino,nosharesock,actimeo=30" >> /etc/fstab'
    sudo mount -t cifs //${AZURE_STORAGE_NAME}.file.core.windows.net/vmfileshare ${VMFILESHARE_PATH} -o credentials=/etc/smbcredentials/${AZURE_STORAGE_NAME}.cred,dir_mode=0777,file_mode=0777,serverino,nosharesock,actimeo=30
}

while true; do
    read -p "Would you like to set the region to GB? " yn
    case $yn in
        [Yy]* ) set_gb_locale ; break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

while true; do
    read -p "Would you like to download Anaconda? " yn
    case $yn in
        [Yy]* ) install_anaconda ; break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

while true; do
    read -p "Would you like to mount vmfileshare (needed for running models)? " yn
    case $yn in
        [Yy]* ) mount_vmfileshare ; break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done
