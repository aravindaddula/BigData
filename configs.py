import os
os_home_dir = os.path.expanduser("~")
# print(os_home_dir)

project_name='BigData'

project_root_dir = f'{os_home_dir}/{project_name}'

'''Configuration details to work with spark'''
SPARK_JARS=f"{project_root_dir}/jars/postgresql-42.7.5.jar, \
             {project_root_dir}/jars/delta-spark_2.12-3.1.0.jar, \
             {project_root_dir}/jars/hadoop-common-3.3.4.jar, \
             {project_root_dir}/jars/delta-storage-3.1.0.jar"


            #  {project_root_dir}/jars/hadoop-aws-3.3.4.jar, \
            #  {project_root_dir}/jars/hadoop-common-3.3.4.jar, \
            #  {project_root_dir}/jars/aws-java-sdk-bundle-1.12.262.jar, \
            #  {project_root_dir}/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar, \
            #  {project_root_dir}/jars/iceberg-aws-bundle-1.9.0.jar"
# print(SPARK_JARS)