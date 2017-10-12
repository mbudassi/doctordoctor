#This first draft makes the distinction between doctor visits and patients seen

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
        return ((str(y['participant'][0]['individual']),str(y['diagnosis'][0]['condition'])),str(y['subject']))
    except:
        return ('0','0')

def main(*argv):

    flipprelim = 0

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

    trunc_fields = StructType([
        StructField("Practitioner", StringType(), True),
        StructField("Patient", StringType(), True),
        StructField("Number of cases", IntegerType(), True)
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

        rdd_encounters = raw_data.map(lambda x: diagnosis_json(x[1]) )\
                                 .groupByKey()\
                                 .mapValues(lambda x: [(r,list(x).count(r)) for r in set(x)] )\
                                 .flatMap(lambda x: [(x[0],r) for r in x[1]])

        total_diag = rdd_encounters.map(lambda x:x[0][1]).distinct().collect()

        if flipprelim:
            index_in="prelim_doc_assess3"
            index_out="prelim_doc_assess4"
            flipprelim = 0
        else:
            index_in="prelim_doc_assess4"
            index_out="prelim_doc_assess3"
            flipprelim = 1

        if es.indices.exists(index=index_out):
            es.indices.delete(index=index_out)

        for ii in total_diag:

            new_input = rdd_encounters.filter(lambda x:x[0][1] == ii)\
                                      .map(lambda x: ((x[0][0],x[1][0]),x[1][1]) )

            from_es_list = []
            
            try:
                result = helpers.scan(es,index=index_in, doc_type=ii)

                for i in result:
                    from_es_list.append(json.dumps(i['_source']))

            except:
                pass

            if from_es_list:
                    
                from_es_rdd = sc.parallelize(from_es_list, 18)
                from_es_df = sqlContext.read\
                                       .json(from_es_rdd, trunc_fields)
            
                openup = from_es_df.rdd\
                                   .map(lambda x:((x[0],x[1]),x[2]) )

                new_input = new_input.union(openup)\
                                     .reduceByKey(add)
        
            new_output = new_input.map(lambda x: [x[0][0],x[0][1],x[1]] )

            es_new_output = sqlContext.createDataFrame(new_output)\
                                  .select(col("_1").alias("Practitioner"),col("_2").alias("Patient"),col("_3").alias("Number of cases"))\
                                  .toJSON()\
                                  .collect()
                                         
            actions = []
            for k in range(len(es_new_output)):
            
                if k and not k%500:
                    helpers.bulk(es, actions)
                    actions = []

                j = json.loads(es_new_output[k])
                j.update({"_index": index_out})
                j.update({"_type": ii})
                j.update({"_id": k})
                actions.append(j)

            if actions:
                helpers.bulk(es, actions)

    if es.indices.exists(index=index_in):
        es.indices.delete(index=index_in)

    doctor_data = sc.hadoopFile('s3a://'+ doctorfile ,\
                                 'org.apache.hadoop.mapred.TextInputFormat',\
                                 'org.apache.hadoop.io.Text',\
                                 'org.apache.hadoop.io.LongWritable')

    rdd_practitioners = doctor_data.map(lambda x:doctor_json(x[1]))

    mapping = es.indices.get_mapping(index=index_out)

    for ii in mapping[index_out]['mappings']:

        result = helpers.scan(es,index=index_out, doc_type=ii)

        from_es_list = []
        for i in result:
            from_es_list.append(json.dumps(i['_source']))

        from_es_rdd = sc.parallelize(from_es_list, 18)
        from_es_df = sqlContext.read\
                               .json(from_es_rdd, trunc_fields)
    
        nonames = from_es_df.rdd

        fullnames_bycase = nonames.map(lambda x: (x[0],x[2]) )\
                                  .reduceByKey(add)

        fullnames_bypatient = nonames.map(lambda x: (x[0],1) )\
                                     .reduceByKey(add)
        
        fullnames = fullnames_bypatient.join(fullnames_bycase)\
                                       .map(lambda x: (x[0],[x[1][0],x[1][1]]) )\
                                       .join(rdd_practitioners)\
                                       .map(lambda x:[x[0],x[1][0][0],x[1][0][1],x[1][1][0],x[1][1][1],x[1][1][2]])

        es_new_output = sqlContext.createDataFrame(fullnames)\
                                 .select(col('_1').alias('Practitioner'),col('_2').alias("Number of patients"),col('_3').alias("Number of cases"),col('_4').alias("Full name"),col('_5').alias("E-mail"),col('_6').alias('Hospital'))\
                                 .toJSON()\
                                 .collect()

        actions = []
        for k in range(len(es_new_output)):

            if k and not k%500:
                helpers.bulk(es, actions)
                actions = []

            j = json.loads(es_new_output[k])
            j.update({"_index": "final_doctor_data3"})
            j.update({"_type": ii})
            j.update({"_id": k})
            actions.append(j)
            
        if actions:
            helpers.bulk(es, actions)

if __name__=="__main__":
    main(*sys.argv)
