from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from operator import add
import os, json, sys, boto
from elasticsearch import Elasticsearch, helpers
from pyspark.sql.types import *

def doctor_json(x):
    try:
        y = json.loads(x)
        return (str(y['identifier']),[str(y['name']),str(y['telecom']),str(y['qualification'][0]['identifier'])])
    except:
        return ('0','0')

def diagnosis_json(x):
    try:
        y = json.loads(x)
        return (str(y['participant'][0]['individual']),str(y['diagnosis'][0]['condition']))
    except:
        return ('0','0')

def main(*argv):

    es_access_key = os.getenv('ES_ACCESS_KEY_ID', 'default')
    es_secret_access_key = os.getenv('ES_SECRET_ACCESS_KEY', 'default')

    master_internal_ip = os.getenv('MASTER_INTERNAL_IP', 'default')
    worker1_internal_ip = os.getenv('WORKER1_INTERNAL_IP', 'default')
    worker2_internal_ip = os.getenv('WORKER2_INTERNAL_IP', 'default')
    worker3_internal_ip = os.getenv('WORKER3_INTERNAL_IP', 'default')

    try:
        es = Elasticsearch(
            [master_internal_ip, worker1_internal_ip, worker2_internal_ip, worker3_internal_ip],
            http_auth=(es_access_key, es_secret_access_key),
            port=9200,
            sniff_on_start=True
        )
        print "Connected"
    except Exception as ex:
        print "Error:", ex
        return

    es_fields = StructType([
        StructField("Practitioner", StringType(), True),
        StructField("Diagnosis", StringType(), True),
        StructField("Number of patients", IntegerType(), True)
    ])

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    conf = SparkConf().setAppName("doctordoctor")
    SparkContext.setSystemProperty('spark.executor.memory', '5g')
    sc = SparkContext(conf=conf)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId",aws_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey",aws_secret_access_key)
    sqlContext = SQLContext(sc)

    doctorfile = 0
    allfiles = []
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    buckets = conn.get_all_buckets()

    for i in buckets:
        for j in i.list():
            if 'mockdata2/' in j.key:
                allfiles.append(i.name + '/' + j.key)
            if 'mockdoctor2/' in j.key:
                doctorfile = i.name + '/' + j.key

    if not doctorfile:
        print "ERROR: NO DOCTOR FILE FOUND"
        return

    for mockdatafile in allfiles:

        raw_data = sc.hadoopFile('s3a://'+mockdatafile,\
                                 'org.apache.hadoop.mapred.TextInputFormat',\
                                 'org.apache.hadoop.io.Text',\
                                 'org.apache.hadoop.io.LongWritable')

        rdd_encounters = raw_data.map(lambda x: (diagnosis_json(x[1]),1))\
                                 .reduceByKey(add)

        from_es_list = []

        try:
            result = helpers.scan(es,index="prelim_doc_assess")

            for i in result:
                j = i['_source']
                j.update({"Diagnosis":i['_type']}) 
                from_es_list.append(json.dumps(j))

        except:
            pass

        if from_es_list:
            
            if es.indices.exists(index="prelim_doc_assess"):
                es.indices.delete(index="prelim_doc_assess")
        
            from_es_rdd = sc.parallelize(from_es_list, 54)
            from_es_df = sqlContext.read\
                                   .json(from_es_rdd, es_fields)
            
            openup = from_es_df.rdd\
                               .map(lambda x:((x[0],x[1]),x[2]))

            rdd_encounters = rdd_encounters.union(openup)\
                                           .reduceByKey(add)
        
        newoutput = rdd_encounters.map(lambda x: [x[0][0],x[0][1],x[1]])

        es_newoutput = sqlContext.createDataFrame(newoutput)\
                                 .select(col("_1").alias("Practitioner"),col("_2").alias("Diagnosis"),col("_3").alias("Number of patients"))\
                                 .toJSON()\
                                 .collect()
            
        actions = []
        for k in range(len(es_newoutput)):

            if k and not k%1000:
                helpers.bulk(es, actions)
                actions = []

            j = json.loads(es_newoutput[k])
            l = j['Diagnosis']
            j.pop('Diagnosis')
            j.update({"_index": "prelim_doc_assess"})
            j.update({"_type": l})
            j.update({"_id": k})
            actions.append(j)

        if actions:
            helpers.bulk(es, actions)

    doctor_data = sc.hadoopFile('s3a://'+ doctorfile ,\
                                 'org.apache.hadoop.mapred.TextInputFormat',\
                                 'org.apache.hadoop.io.Text',\
                                 'org.apache.hadoop.io.LongWritable')

    rdd_practitioners = doctor_data.map(lambda x:doctor_json(x[1]))

    mapping = es.indices.get_mapping(index="prelim_doc_assess")

    for ii in mapping['prelim_doc_assess']['mappings']:

        result = helpers.scan(es,index="prelim_doc_assess", doc_type=ii)

        from_es_list = []
        for i in result:
            j = i['_source']
            j.update({"Diagnosis":i['_type']}) 
            from_es_list.append(json.dumps(j))

        from_es_rdd = sc.parallelize(from_es_list, 54)
        from_es_df = sqlContext.read\
                               .json(from_es_rdd, es_fields)
    
        fullnames = from_es_df.rdd\
                              .map(lambda x: (x[0],[x[1],x[2]]))\
                              .join(rdd_practitioners)\
                              .map(lambda x:[x[0],x[1][0][1],x[1][1][0],x[1][1][1],x[1][1][2],x[1][0][0]])

        es_newoutput = sqlContext.createDataFrame(fullnames)\
                                 .select(col('_1').alias('Practitioner'),col('_2').alias("Number of patients"),col('_3').alias("Full name"),col('_4').alias("E-mail"),col('_5').alias('Hospital'),col('_6').alias('Diagnosis'))\
                                 .toJSON()\
                                 .collect()

        actions = []
        for k in range(len(es_newoutput)):

            if k and not k%1000:
                helpers.bulk(es, actions)
                actions = []

            j = json.loads(es_newoutput[k])
            j.pop('Diagnosis')
            j.update({"_index": "final_doctor_data"})
            j.update({"_type": ii})
            j.update({"_id": k})
            actions.append(j)
            
        if actions:
            helpers.bulk(es, actions)

if __name__=="__main__":
    main(*sys.argv)
