import os
from elasticsearch import Elasticsearch, helpers

def callelastic(queries):

    es_access_key = os.getenv('ES_ACCESS_KEY_ID', 'default')
    es_secret_access_key = os.getenv('ES_SECRET_ACCESS_KEY', 'default')
    probable_disease = 0

    try:
        es = Elasticsearch(
            ["10.0.0.14", "10.0.0.4", "10.0.0.8", "10.0.0.11"],
            http_auth=(es_access_key, es_secret_access_key),
            port=9200,
        )
        print "Connected"
    except Exception as ex:
        print "Error:", ex

    groupit = []
    setit = []

    x = []
    result1 = helpers.scan(es, index="final_doctor_data",query={\
                                    "query":{\
                                    "match":{\
                                    "_type":{\
                                    "query": queries[0],\
                                    "boost" : 1.0,\
                                    "fuzziness" : 2,\
                                    "prefix_length" : 0,\
                                    "max_expansions": 50}}}})

    try:
        for i in result1:
            if not probable_disease:
                probable_disease = i['_type']

            x.append((i['_source']['Full name'],i['_source']['Number of patients'],i[\
'_source']['Hospital'],i['_source']['E-mail']))

    except:
        pass

    groupit.append(x)
    setit+=x

    y = []
    result2 = helpers.scan(es, index="final_doctor_data",query={\
                                    "query": {\
                                    "fuzzy":{\
                                    "Full name": queries[1] }}})

    try:
        for i in result2:
            y.append((i['_source']['Full name'],i['_source']['Number of patients'],i['_s\
ource']['Hospital'],i['_source']['E-mail']))

    except:
            pass

    groupit.append(y)
    setit+=y

    z = []
    result3 = helpers.scan(es, index="final_doctor_data",query={\
                                    "query": {\
                                    "fuzzy":{\
                                    "Hospital": str(queries[2]) }}})

    try:
        for i in result3:
            z.append((i['_source']['Full name'],i['_source']['Number of patients'],i['_s\
ource']['Hospital'],i['_source']['E-mail']))

    except:
            pass

    groupit.append(z)
    setit+=z

    full_setit = set(setit)

    for i in groupit:
        if full_setit.intersection(set(i)):
            full_setit = full_setit.intersection(set(i))

    returnit = list(full_setit)

    returnit.sort(key=lambda x:x[1], reverse=True)

    fulloutput = []

    if probable_disease:
        fulloutput.append("Disease Query: " + queries[0])
        fulloutput.append("Searching Disease: " + probable_disease)
        fulloutput.append("\nResults:\n")
        if returnit:
            z = returnit[:10]
            for i in z:
                fulloutput.append('Dr. ' + str(i[0]) + ', ' + str(i[1]) + ' patients, ' + str(i[2]) + ', ' + str(i[3]))
        else:
            fulloutput.append("NONE")
    else:
        fulloutput.append("\nResults:\n")
        if returnit:
            z = set()
            for i in returnit:
                z.update(['Dr. ' + str(i[0]) + ', ' + str(i[2]) + ', ' + str(i[3])])
                if len(z) > 9:
                    break
            for i in z:
                fulloutput.append(i)
        else:
            fulloutput.append("NONE")

    return fulloutput
