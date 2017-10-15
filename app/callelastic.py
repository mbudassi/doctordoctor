#This is the elasticsearch module that the flask webapp accesses to answer queries

import os, logging
from elasticsearch import Elasticsearch, helpers

def callelastic(queries):

    probable_disease = 0

    #Setup logging. Write log to home directory
    homedir = os.getenv('HOME', 'default')
    logging.basicConfig(filename=homedir +'/callelastic.log',filemode='w', level=logging.DEBUG)

    #Get elasticsearch credentials from environmental variables
    es_access_key = os.getenv('ES_ACCESS_KEY_ID', 'default')
    es_secret_access_key = os.getenv('ES_SECRET_ACCESS_KEY', 'default')

    #Get elasticsearch internal IP addresses from environmental variables
    master_internal_ip = os.getenv('MASTER_INTERNAL_IP', 'default')
    worker1_internal_ip = os.getenv('WORKER1_INTERNAL_IP', 'default')
    worker2_internal_ip = os.getenv('WORKER2_INTERNAL_IP', 'default')
    worker3_internal_ip = os.getenv('WORKER3_INTERNAL_IP', 'default')

    #Open a connection to Elasticsearch
    try:
        es = Elasticsearch(
            [master_internal_ip, worker1_internal_ip, worker2_internal_ip, worker3_internal_ip],
            http_auth=(es_access_key, es_secret_access_key),
            port=9200,
            sniff_on_start=True
        )
        logging.debug("Elasticsearch Connected")
    except Exception as ex:
        logging.debug("Error:", ex)
        return

    #groupit collects lists. setit creates one long list
    groupit = []
    setit = []

    #The first search term: Diagnosis
    x = []
    result1 = helpers.scan(es, index="final_doctor_data",query={\
                                    "query":{\
                                    "match":{\
                                    "_type":{\
                                    "query": queries[0].lower(),\
                                    "boost" : 1.0,\
                                    "fuzziness" : 2,\
                                    "prefix_length" : 0,\
                                    "max_expansions": 50}}}})

    try:
        for i in result1:
            #Set the confirmed diagnosis to display alonside user input (So that the user can see what they typed and what the app understood)
            if not probable_disease:
                probable_disease = i['_type']

            x.append((i['_source']['Full name'],i['_source']['Number of patients'],i['_source']['Hospital'],i['_source']['E-mail']))

    except:
        pass

    groupit.append(x)
    setit+=x

    #The second search term: Doctor name
    y = []
    result2 = helpers.scan(es, index="final_doctor_data",query={\
                                    "query": {\
                                    "match":{\
                                    "Full name":{\
                                    "query": queries[1].lower(),\
                                    "boost" : 1.0,\
                                    "fuzziness" : 2,\
                                    "prefix_length" : 0,\
                                    "max_expansions": 50}}}})

    try:
        for i in result2:
            y.append((i['_source']['Full name'],i['_source']['Number of patients'],i['_source']['Hospital'],i['_source']['E-mail']))

    except:
            pass

    groupit.append(y)
    setit+=y

    #The third search term: Hospital name
    z = []
    result3 = helpers.scan(es, index="final_doctor_data",query={\
                                    "query": {\
                                    "match":{\
                                    "Hospital":{\
                                    "query": queries[2].lower(),\
                                    "boost" : 1.0,\
                                    "fuzziness" : 1,\
                                    "prefix_length" : 0,\
                                    "max_expansions": 50}}}})

    try:
        for i in result3:
            z.append((i['_source']['Full name'],i['_source']['Number of patients'],i['_source']['Hospital'],i['_source']['E-mail']))

    except:
            pass

    groupit.append(z)
    setit+=z

    full_setit = set(setit)

    #Find the common entries within the three lists returned by the three queries. If a list is empty, ignore it
    for i in groupit:
        if full_setit.intersection(set(i)):
            full_setit = full_setit.intersection(set(i))

    returnit = list(full_setit)

    #Sort the list, maximum element first
    returnit.sort(key=lambda x:x[1], reverse=True)

    fulltitles = []
    fulloutput = []

    #Create return value for Flask app. Diagnosis is only returned if it was part of the original query
    if probable_disease:
        fulltitles.append("Disease Query: " + queries[0])
        fulltitles.append("Searching Disease: " + probable_disease)
        fulltitles.append("\nResults:\n")
        if returnit:
            z = returnit[:10]
            for i in z:
                fulloutput.append('Dr. ' + str(i[0]) + ', ' + str(i[1]) + ' patients, ' + str(i[2].title()) + ', ' + str(i[3]))
        else:
            fulloutput.append("NONE")
    else:
        fulltitles.append("\nResults:\n")
        if returnit:
            z = set()
            for i in returnit:
                z.update(['Dr. ' + str(i[0]) + ', ' + str(i[2].title()) + ', ' + str(i[3])])
                if len(z) > 9:
                    break
            for i in z:
                fulloutput.append(i)
        else:
            fulloutput.append("NONE")

    #fulltitles begins the results list. fulloutput contain the actual results 
    return [fulltitles, fulloutput]
