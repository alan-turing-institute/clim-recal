#!/bin/bash
# Basic if statement

CHECKOUT_PATH=$HOME/code/clim-recal
ANACONDA_INSTALL_FOLDER=$HOME/code/anaconda-install
ANACONDA_INSTALL_SCRIPT_FILE_NAME=Anaconda3-2023.07-2-Linux-x86_64.sh
ANACONDA_INSTALL_URL=https://repo.anaconda.com/archive/$ANACONDA_INSTALL_SCRIPT_FILE_NAME			
sudo apt-get update && sudo apt-get -y install locales gdal-bin python3-gdal libgdal-dev build-essential wget && sudo apt-get upgrade

while true; do
    read -p "Would you like to set the region to GB? " yn
    case $yn in
        [Yy]* ) sudo echo "en_GB.UTF-8 UTF-8" > /etc/locale.gen && locale-genmake install; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

cd $CHECKOUT_PATH/python/debiasing && git submodule update --init --recursive

while true; do
    read -p "Would you like to dowload Anaconda? " yn
    case $yn in
        [Yy]* ) mkdir -p $ANACONDA_INSTALL_PATH; cd $ANACONDA_INSTALL_PATH; wget $ANACONDA_INSTALL_URL; bash $ANACONDA_INSTALL_SCRIPT_FILE_NAME ; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done
