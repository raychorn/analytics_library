#!/bin/bash
###################################################################################
# smsi-hive.sh
#
# This scripts builds all of the components for hive according to our SMSI
# Structure, which is based on Apache's structure. The components are:
#     Derby - for standalone metadata access
#     Hive - SQL wrapper for hadoop
#
# Options to build from Trunk or Existing Tags and/or Brances of existing verions.
#
##################################################################################
#
##################################################################################
usage()
{
cat << EOF
	usage: $0 [arguments] [options]
	example: $0 -ib -s branches -v 0.6 -d $HOME/analytics-thirdparty/hive

	This script builds smsi hive.
	
	ARGUMENTS:
	  -b		build - downloads and builds hive.
	  -i		install/deploy - deploys the installed hive sources and libraries to the specified location.

	OPTIONS:
	  -h      Show this message
	  -s	  Source (branches [default] | tag | trunk)
	  -v      Version (0.6 [default] | 0.5 | trunk)
	  -d	  Install Directory ($HOME/analytics-thirdparty/hive [default])
	  -n	  Destination Node (Master Node: hostname [default] | hivedev1 | hive1 | hive3)
EOF
}
#Check the number of arguments and exit if it's wrong
if [ $# -lt 1 ]
then
	usage
	exit 1
fi
INSTALL=
BUILD=
SOURCE="branches"
VERSION="0.6"
INSTALL_DIR="${HOME}/analytics-thirdparty/hive"
NODE=$HOSTNAME
while getopts "ibhs:v:d:n:" OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
		 i)
		     INSTALL="true"
		     ;;
		 b)
			 BUILD="true"
			 ;;
         s)
             SOURCE=$OPTARG
             ;;
         v)
             VERSION=$OPTARG
             ;;
         d)
             INSTALL_DIR=$OPTARG
             ;;
		 n)
	         NODE=$OPTARG
	         ;;
         ?)
             usage
             exit
             ;;
     esac
done

# Check if Install & Build not passed as arguments
if [ -z $INSTALL ] && [ -z $BUILD ]
then
     usage
     exit 1
fi

# Check for ANALYTICS_LIB if NULL exit with error
if [ -z $ANALYTICS_LIB ]
then
	echo "error: ANALTYCS_LIB path not specified, usually 'export ANALYTICS_LIB=/home/hadoop/analytics-thirdparty/lib' should work"
	exit 1
fi

#===========#
# BUILD     #
#===========#
if [ $BUILD = "true" ]; then
	##########################################
	# Trunk
	##########################################
	if [ $SOURCE = "trunk" ]; then
		echo "Building Hive From Trunk"
		echo "IMPLEMENTATION PENDING"
		exit 1
	##########################################
	# Branch
	##########################################
	elif [ $SOURCE = "branches" ]; then
		# Check for VERSION if NULL exit with error
		if [ -z $VERSION ]
		then
				echo "error: VERSION not specified, check the available versions on the corresponding directory, each version needs to be the same for each component of hive."
				usage
				exit 1
		fi
		case $VERSION in
	     	0.5*)
				echo "Building Hive From Branch 0.5"
				echo "IMPLEMENTATION PENDING"
				exit 1
			;;
			0.6*)
				echo "Building Hive From Branch 0.6"
				# Check out sources
				echo "checking out required sources from subversion...enter credentials if asked..."
				SVN_LOG=$(svn co https://mmeng.smithmicro.net/repos/main/analytics-thirdparty/hive/${SOURCE}/hive-${VERSION} ${INSTALL_DIR}/${SOURCE}/hive-${VERSION}) || { echo "Subversion Error. Will not continue. Check the LOG."; echo "${SVN_LOG}" > INSTALL_LOG.log; exit 1; }
				echo "processing..."
				SVN_LOG=$(svn co https://mmeng.smithmicro.net/repos/main/analytics-thirdparty/hive/etc ${INSTALL_DIR}/etc) || { echo "Subversion Error. Will not continue. Check the LOG."; echo "${SVN_LOG}" >> INSTALL_LOG.log; exit 1; }
				echo "...checking out derby:db..."
				SVN_LOG=$(svn co https://mmeng.smithmicro.net/repos/main/analytics-thirdparty/db/derby/code/tags/db-derby-10.5.1.1-bin ${INSTALL_DIR}/derby) || { echo "Subversion Error. Will not continue. Check the LOG."; echo "${SVN_LOG}" >> INSTALL_LOG.log; exit 1; }	
				echo "subversion done."
				
				# Setup Derby
				echo ""
				echo "Setting up Derby:DB"
				sleep 3
				mkdir ${INSTALL_DIR}/derby/data
				pushd ${INSTALL_DIR}/derby
				DERBY_HOME=${INSTALL_DIR}/derby nohup ${INSTALL_DIR}/derby/bin/startNetworkServer -h 0.0.0.0 & 
				cat nohup.out
				popd
				echo "...if derby did not start properly, you may start it at another time..."
				echo "done."
				sleep 3
				
				# build hive
				echo ""
				echo "building hive..."
				sleep 3
				ANT_LOG=$(ant -f ${INSTALL_DIR}/$SOURCE/hive-$VERSION/build.xml clean) || { echo "Build Error. Will not continue. Check the ANT LOG."; echo "${ANT_LOG}" >> INSTALL_LOG.log; exit 1; }
				echo "...packaging hive...(this may take a while)..."
				ANT_LOG=$(ant -f ${INSTALL_DIR}/$SOURCE/hive-$VERSION/build.xml package) || { echo "Build Error while packaging. Will not continue. Re-run installation and make sure you have ANT version 1.7.1 or greater. Check the LOG."; echo "${ANT_LOG}" >> INSTALL_LOG.log; exit 1; }
				echo "done."
			;;
		esac
	fi
fi

#===========#
# INSTALL   #
#===========#
if [ $INSTALL = "true" ]; then
	##########################################
	# Trunk
	##########################################
	if [ $SOURCE = "trunk" ]; then
		echo "Deploying Hive From Trunk"
		echo "IMPLEMENTATION PENDING"
		exit 1
	##########################################
	# Branch
	##########################################
	elif [ $SOURCE = "branches" ]; then
		# Check for VERSION if NULL exit with error
		if [ -z $VERSION ]
		then
			echo "error: VERSION not specified, check the available versions on the corresponding directory, each version needs to be the same for each component of hadoop."
			exit 1
		fi
		case $VERSION in
			0.5*)
				echo "Deploying Hive From Branches 0.5"
				echo "IMPLEMENTATION PENDING"
				exit 1
			;;
			0.6*)
				echo "Deploying Hive From Branches 0.6"
				sleep 3
				mkdir ${INSTALL_DIR}/build ${INSTALL_DIR}/conf ${INSTALL_DIR}/bin
				# Hive - Copy the binaries and configurations
				cp -Rf ${INSTALL_DIR}/$SOURCE/hive-$VERSION/bin/* ${INSTALL_DIR}/bin
				cp -Rf ${INSTALL_DIR}/$SOURCE/hive-$VERSION/conf/* ${INSTALL_DIR}/conf
				cp -Rf ${INSTALL_DIR}/$SOURCE/hive-$VERSION/build/* ${INSTALL_DIR}/build
				cp -Rf ${INSTALL_DIR}/$SOURCE/hive-$VERSION/lib ${INSTALL_DIR}/
				cp -Rf ${INSTALL_DIR}/build/dist/lib/* ${INSTALL_DIR}/lib
				# Copy the smsi templates from etc folder and replace original
				cp -Rf ${INSTALL_DIR}/etc/hive-site.xml.smsi.${NODE}.template ${INSTALL_DIR}/conf/hive-site.xml
				cp -Rf ${INSTALL_DIR}/etc/jpox.properties.smsi.${NODE}.template ${INSTALL_DIR}/conf/jpox.properties
				
				echo "Done."
				
				echo ""
				echo "Caveats:"
				echo "==============="
				echo "If you get invalid heap size:"
				echo "/t1. In ${INSTALL_DIR}/bin/ext/util directory, open execHiveCmd.sh"
				echo "/t2. Edit HADOOP_HEAPSIZE=4096 (to): 1024"
				
				echo "Now please set the new Environment Variables for your deployment:"
				echo "Similar to:"
				echo "\texport HIVE_HOME=\"${INSTALL_DIR}\""
				echo "\texport PATH=\"\$HIVE_HOME/bin:\$PATH\""
			;;
		esac
	##########################################
	# Clean
	##########################################
	elif [ $SOURCE = "clean" ]; then	
		echo "cleaning..."
		rm -rf ${INSTALL_DIR}/build
		rm -rf ${INSTALL_DIR}/bin
		rm -rf ${INSTALL_DIR}/conf
		rm -rf ${INSTALL_DIR}/lib
		echo "removed build/* bin/* conf/*"
		echo "cleaned!"
	fi
fi
