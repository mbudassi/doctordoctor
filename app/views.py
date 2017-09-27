import os
from flask import render_template, request
from app import app
import sys
from elasticsearch import Elasticsearch, helpers

def getit (getitin):

    try:
        es = Elasticsearch(
            ["10.0.0.14", "10.0.0.4", "10.0.0.8", "10.0.0.11"],
            http_auth=('elastic', 'changeme'),
            port=9200,
        )
        print "Connected"
    except Exception as ex:
        print "Error:", ex

    groupit = []
    setit = []

    x = []
    result1 = helpers.scan(es, index="final_doctor_data2",query={\
                                    "query":{\
                                    "fuzzy":{\
                                    "_type": getitin[0] }}})

    try:
        for ii in result1:
            x.append((ii['_source']['Full name'],ii['_source']['Number of patients'],ii[\
'_source']['Hospital'],ii['_source']['E-mail']))

    except:
        pass

    groupit.append(x)
    setit+=x

    y = []
    result2 = helpers.scan(es, index="final_doctor_data2",query={\
                                    "query": {\
                                    "fuzzy":{\
                                    "Full name": getitin[1] }}})

    try:
        for i in result2:
            y.append((i['_source']['Full name'],i['_source']['Number of patients'],i['_s\
ource']['Hospital'],i['_source']['E-mail']))

    except:
            pass

    groupit.append(y)
    setit+=y

    z = []
    result3 = helpers.scan(es, index="final_doctor_data2",query={\
                                    "query": {\
                                    "fuzzy":{\
                                    "Hospital": str(getitin[2]) }}})

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

    fulloutput.append("Query: " + str(getitin[0]))
    fulloutput.append("Disease: " + str(ii['_type']))
    fulloutput.append("\nResults:\n")
    if returnit:
        z = returnit[:10]
        for i in z:
            fulloutput.append('Dr. ' + str(i[0]) + ', ' + str(i[1]) + ' patients, ' + str(i[2]) + ', ' + str(i[3]))
    else:
        fulloutput.append("NONE")

    return fulloutput

@app.route('/')
@app.route('/index')
def index():
   return render_template("index.html")

@app.route('/query')
def query():
	return render_template("query.html")

@app.route("/query", methods=['POST'])
def query_post():
    emailid1 = request.form["emailid1"]
    emailid2 = request.form["emailid2"]
    emailid3 = request.form["emailid3"]
    x = getit([emailid1, emailid2, emailid3])
    return render_template("queryresponse.html", array=x)
