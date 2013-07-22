#!/bin/bash
###################################################################################
# build-smsi-hadoop.sh
#
# This scripts builds all of the components for hadoop according to our SMSI
# Structure, which is based on Apache's structure. The three components are:
#     Core - common
#     MapReduce - mapreduce
#     HDFS - hdfs
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
	example: $0 -ib -s branches -v 0.20-append -d $HOME/analytics-thirdparty/hadoop

	This script builds smsi hadoop.
	
	ARGUMENTS:
	  -b		build - downloads and builds hadoop.
	  -i		install/deploy - deploys the installed hadoop sources and libraries to the specified location.

	OPTIONS:
	  -h      Show this message
	  -s	  Source (branches [default] | tag | trunk)
	  -v      Version (0.20-append [default] | 0.21 | trunk)
	  -d	  Install Directory ($HOME/analytics-thirdparty/hadoop [default])
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
VERSION="0.20-append"
INSTALL_DIR="${HOME}/analytics-thirdparty/hadoop"
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
		echo "Building Hadoop From Trunk"
		echo "IMPLEMENTATION PENDING"
		exit 1
		###NEEDS UPDATES (OLD)
		# link properties
		#ln -f -s $ANALYTICS_LIB/../hadoop/etc/build-trunk.properties common/trunk/build.properties
		#ln -f -s $ANALYTICS_LIB/../hadoop/etc/build-trunk.properties hdfs/trunk/build.properties
		#ln -f -s $ANALYTICS_LIB/../hadoop/etc/build-trunk.properties mapreduce/trunk/build.properties
		#
		# build common (hadoop)
		#ant -f common/trunk/build.xml clean
		#ant -f common/trunk/build.xml jar jar-test
		#ln -f -s -t hdfs/trunk/lib $ANALYTICS_LIB/../hadoop/common/trunk/build/hadoop-common-0.21.0.jar
		#ln -f -s -t hdfs/trunk/lib $ANALYTICS_LIB/../hadoop/common/trunk/build/hadoop-common-test-0.21.0.jar
		#ln -f -s -t mapreduce/trunk/lib $ANALYTICS_LIB/../hadoop/common/trunk/build/hadoop-common-0.21.0.jar
		#ln -f -s -t mapreduce/trunk/lib $ANALYTICS_LIB/../hadoop/common/trunk/build/hadoop-common-test-0.21.0.jar
		#
		# build hdfs
		#ant -f hdfs/trunk/build.xml clean
		#ant -f hdfs/trunk/build.xml jar jar-test
		#ln -f -s -t mapreduce/trunk/lib $ANALYTICS_LIB/../hadoop/hdfs/trunk/build/hadoop-hdfs-0.21.0.jar
		#ln -f -s -t mapreduce/trunk/lib $ANALYTICS_LIB/../hadoop/hdfs/trunk/build/hadoop-hdfs-test-0.21.0.jar
		#
		# build mapreduce
		#ant -f mapreduce/trunk/build.xml clean
		#ant -f mapreduce/trunk/build.xml jar jar-test
		#ln -f -s -t hdfs/trunk/lib $ANALYTICS_LIB/../hadoop/mapreduce/trunk/build/hadoop-mapred-0.21.0.jar
		#ln -f -s -t hdfs/trunk/lib $ANALYTICS_LIB/../hadoop/mapreduce/trunk/build/hadoop-mapred-test-0.21.0.jar
	##########################################
	# Branch
	##########################################
	elif [ $SOURCE = "branches" ]; then
		# Check for VERSION if NULL exit with error
		if [ -z $VERSION ]
		then
				echo "error: VERSION not specified, check the available versions on the corresponding directory, each version needs to be the same for each component of hadoop."
				usage
				exit 1
		fi
		case $VERSION in
	     	0.21*)
				echo "Building Hadoop From Branch 0.21"
				echo "IMPLEMENTATION PENDING"
				exit 1
				###NEEDS UPDATES (OLD)
				# link properties
				#ln -f -s $ANALYTICS_LIB/../hadoop/etc/build-$SOURCE.properties common/$SOURCE/common-$VERSION/build.properties
				#ln -f -s $ANALYTICS_LIB/../hadoop/etc/build-$SOURCE.properties hdfs/$SOURCE/hdfs-$VERSION/build.properties
				#ln -f -s $ANALYTICS_LIB/../hadoop/etc/build-$SOURCE.properties mapreduce/$SOURCE/mapreduce-$VERSION/build.properties
				#
				# build common (hadoop)
				#ant -f common/$SOURCE/common-$VERSION/build.xml clean
				#ant -f common/$SOURCE/common-$VERSION/build.xml jar jar-test
				#ln -f -s -t hdfs/$SOURCE/hdfs-$VERSION/lib $ANALYTICS_LIB/../hadoop/common/$SOURCE/common-$VERSION/build/hadoop-common-*.jar
				#ln -f -s -t hdfs/$SOURCE/hdfs-$VERSION/lib $ANALYTICS_LIB/../hadoop/common/$SOURCE/common-$VERSION/build/hadoop-common-test-*.jar
				#ln -f -s -t mapreduce/$SOURCE/mapreduce-$VERSION/lib $ANALYTICS_LIB/../hadoop/common/$SOURCE/common-$VERSION/build/hadoop-common-*.jar
				#ln -f -s -t mapreduce/$SOURCE/mapreduce-$VERSION/lib $ANALYTICS_LIB/../hadoop/common/$SOURCE/common-$VERSION/build/hadoop-common-test-*.jar
				#
				# build hdfs
				#ant -f hdfs/$SOURCE/hdfs-$VERSION/build.xml clean
				#ant -f hdfs/$SOURCE/hdfs-$VERSION/build.xml jar jar-test
				#ln -f -s -t mapreduce/$SOURCE/mapreduce-$VERSION/lib $ANALYTICS_LIB/../hadoop/hdfs/$SOURCE/hdfs-$VERSION/build/hadoop-hdfs-*.jar
				#ln -f -s -t mapreduce/$SOURCE/mapreduce-$VERSION/lib $ANALYTICS_LIB/../hadoop/hdfs/$SOURCE/hdfs-$VERSION/build/hadoop-hdfs-test-*.jar
				#
				# build mapreduce
				#ant -f mapreduce/$SOURCE/mapreduce-$VERSION/build.xml clean
				#ant -f mapreduce/$SOURCE/mapreduce-$VERSION/build.xml jar jar-test
				#ln -f -s -t hdfs/$SOURCE/hdfs-$VERSION/lib $ANALYTICS_LIB/../hadoop/mapreduce/$SOURCE/mapreduce-$VERSION/build/hadoop-mapred-*.jar
				#ln -f -s -t hdfs/$SOURCE/hdfs-$VERSION/lib $ANALYTICS_LIB/../hadoop/mapreduce/$SOURCE/mapreduce-$VERSION/build/hadoop-mapred-test-*.jar
			;;
			0.20*)
				echo "Building Hadoop From Branch 0.20"
				#Check out sources
				echo "checking out required sources from subversion...enter credentials if asked..."
				SVN_LOG=$(svn co https://mmeng.smithmicro.net/repos/main/analytics-thirdparty/hadoop/common/${SOURCE}/common-${VERSION} ${INSTALL_DIR}/common/${SOURCE}/common-${VERSION}) || { echo "Subversion Error. Will not continue. Check the LOG."; echo "${SVN_LOG}" > INSTALL_LOG.log; exit 1; }
				echo "processing..."
				SVN_LOG=$(svn co https://mmeng.smithmicro.net/repos/main/analytics-thirdparty/hadoop/etc ${INSTALL_DIR}/etc) || { echo "Subversion Error. Will not continue. Check the LOG."; echo "${SVN_LOG}" >> INSTALL_LOG.log; exit 1; }
				echo "subversion done."
				
				# build hadoop
				echo "building hadoop..."
				sleep 3
				ANT_LOG=$(ant -f ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/build.xml clean) || { echo "Build Error. Will not continue. Check the ANT LOG."; echo "${ANT_LOG}" >> INSTALL_LOG.log; exit 1; }
				patch -p0 -d ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/ < ${INSTALL_DIR}/etc/HADOOP-5203-md5-0.20.2.patch
				echo "...hadoop jars..."
				ANT_LOG=$(ant -f ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/build.xml jar jar-test) || { echo "Build Error while creating Jars. Will not continue. Check the LOG."; echo "${ANT_LOG}" >> INSTALL_LOG.log; exit 1; }
				echo "done."
				
				# build streaming job jar
				echo ""
				echo "building hadoop streaming jars..."
				sleep 3
				ANT_LOG=$(ant -f ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/src/contrib/streaming/build.xml compile) || { echo "Streaming Build Error. Will not continue. Check the LOG."; echo "${ANT_LOG}" >> INSTALL_LOG.log; exit 1; }
				ANT_LOG=$(ant -f ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/src/contrib/streaming/build.xml jar) || { echo "Streaming Build Error while building Jars. Will not continue. Check the LOG."; echo "${ANT_LOG}" >> INSTALL_LOG.log; exit 1; }
				sleep 3
				LOG=$(mv ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/build/contrib/streaming/hadoop-streaming* ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/build/contrib/streaming/hadoop-$VERSION-streaming.jar) || { echo "Error while renaming streaming jar. You must move contrib/streaming/hadoop-streaming* to your build directory."; echo "${LOG}" >> INSTALL_LOG.log; }
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
		echo "Deploying Hadoop From Trunk"
		echo "IMPLEMENTATION PENDING"
		exit 1
		###NEEDS UPDATES (OLD)
		#mkdir build conf bin
		#Hadoop - common
		#cp -Rf common/trunk/bin/* bin
		#cp -Rf common/trunk/conf/* conf
		#cp -Rf common/trunk/build/* build
		#Hadoop - hdfs
		#cp -Rf hdfs/trunk/bin/* bin
		#cp -Rf hdfs/trunk/conf/* conf
		#cp -Rf hdfs/trunk/build/* build
		#Hadoop - mapreduce
		#cp -Rf mapreduce/trunk/bin/* bin
		#cp -Rf mapreduce/trunk/conf/* conf
		#cp -Rf mapreduce/trunk/build/* build
		#
		#Copy the smsi templates from etc folder and replace original
		#cp -Rf etc/core-site.xml.smsi.template conf/core-site.xml
		#cp -Rf etc/hdfs-site.xml.smsi.template conf/hdfs-site.xml
		#cp -Rf etc/mapred-site.xml.smsi.template conf/mapred-site.xml
		#
		#echo "Now please set the new Environment Variables for your deployment:"
		#echo "Similar to:"
		#echo "\texport HADOOP_HOME=$(pwd)"
		#echo "\texport HADOOP_COMMON_HOME=$(pwd)/common/SOURCE"
		#echo "\texport HADOOP_HDFS_HOME=$(pwd)/hdfs/SOURCE"
		#echo "\texport HADOOP_MAPRED_HOME=$(pwd)/mapreduce/SOURCE"
		#echo "\texport HADOOP_CONF_DIR=\$HADOOP_HOME/conf"
		#echo "\texport PATH=\$PATH:\$HADOOP_HOME/bin"
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
	        # Check for DESTINATION if NULL exit with error
	        #if [ -z $DESTINATION ]
	        #then
	        #        echo "error: DESTINATION not specified, please specify a DESTINATION PATH if current directory then use ./"
	        #        exit 1
	        #fi
		case $VERSION in
			0.21*)
				echo "Deploying Hadoop From Branches 0.21"
				echo "IMPLEMENTATION PENDING"
				exit 1
				###NEEDS UPDATES (OLD)
				#mkdir build conf bin
				#Hadoop - common
				#cp -Rf common/$SOURCE/common-$VERSION/bin/* bin
				#cp -Rf common/$SOURCE/common-$VERSION/conf/* conf
				#cp -Rf common/$SOURCE/common-$VERSION/build/* build
				#Hadoop - hdfs
				#cp -Rf hdfs/$SOURCE/hdfs-$VERSION/bin/* bin
				#cp -Rf hdfs/$SOURCE/hdfs-$VERSION/conf/* conf
				#cp -Rf hdfs/$SOURCE/hdfs-$VERSION/build/* build
				#Hadoop - mapreduce
				#cp -Rf mapreduce/$SOURCE/mapreduce-$VERSION/bin/* bin
				#cp -Rf mapreduce/$SOURCE/mapreduce-$VERSION/conf/* conf
				#cp -Rf mapreduce/$SOURCE/mapreduce-$VERSION/build/* build
				#
				#Copy the smsi templates from etc folder and replace original
				#cp -Rf etc/core-site.xml.smsi.template conf/core-site.xml
				#cp -Rf etc/hdfs-site.xml.smsi.template conf/hdfs-site.xml
				#cp -Rf etc/mapred-site.xml.smsi.template conf/mapred-site.xml
				#cp -Rf etc/hadoop-env.sh.smsi.template conf/hadoop-env.sh
				#cp -Rf etc/masters.smsi.staging.template conf/masters
				#cp -Rf etc/slaves.smsi.staging.template conf/slaves
				#
				#echo "Now please set the new Environment Variables for your deployment:"
				#echo "Similar to:"
				#echo "\texport HADOOP_HOME=$(pwd)"
				#echo "\texport HADOOP_COMMON_HOME=$(pwd)/common/SOURCE"
				#echo "\texport HADOOP_HDFS_HOME=$(pwd)/hdfs/SOURCE"
				#echo "\texport HADOOP_MAPRED_HOME=$(pwd)/mapreduce/SOURCE"
				#echo "\texport HADOOP_CONF_DIR=\$HADOOP_HOME/conf"
				#echo "\texport PATH=\$PATH:\$HADOOP_HOME/bin"
			;;
			0.20*)
				echo "Deploying Hadoop From Branches 0.20"
				sleep 3
				mkdir ${INSTALL_DIR}/build ${INSTALL_DIR}/conf ${INSTALL_DIR}/bin
				# Hadoop - Copy the binaries and configurations
				cp -Rf ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/bin/* ${INSTALL_DIR}/bin
				cp -Rf ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/conf/* ${INSTALL_DIR}/conf
				cp -Rf ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/build/* ${INSTALL_DIR}/build
				cp -Rf ${INSTALL_DIR}/common/$SOURCE/common-$VERSION/lib ${INSTALL_DIR}/
				# Copy the smsi templates from etc folder and replace original
				cp -Rf ${INSTALL_DIR}/etc/core-site.xml.smsi.${NODE}.template ${INSTALL_DIR}/conf/core-site.xml
				cp -Rf ${INSTALL_DIR}/etc/hdfs-site.xml.smsi.${NODE}.template ${INSTALL_DIR}/conf/hdfs-site.xml
				cp -Rf ${INSTALL_DIR}/etc/mapred-site.xml.smsi.${NODE}.template ${INSTALL_DIR}/conf/mapred-site.xml
				cp -Rf ${INSTALL_DIR}/etc/hadoop-env.sh.smsi.template ${INSTALL_DIR}/conf/hadoop-env.sh
				#cp -Rf etc/masters.smsi.staging.template conf/masters
				#cp -Rf etc/slaves.smsi.staging.template conf/slaves
				mkdir ${HOME}/bin
				ln -s ${INSTALL_DIR} ${HOME}/bin/hadoop

				echo "Now please set the new Environment Variables for your deployment:"
				echo "Similar to:"
				echo "\texport HADOOP_HOME=\"${INSTALL_DIR}\""
				echo "\texport HADOOP_CONF_DIR=\"\$HADOOP_HOME/conf\""
				echo "\texport PATH=\"\$HADOOP_HOME/bin:\$PATH\""
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
