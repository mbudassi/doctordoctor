from callelastic import callelastic
from flask import render_template, request
from app import app

@app.route('/')
def query():
	return render_template("query.html")

@app.route("/", methods=['POST'])
def query_post():
    query1 = request.form["query1"]
    query2 = request.form["query2"]
    query3 = request.form["query3"]
    responselist = callelastic([query1, query2, query3])
    return render_template("queryresponse.html", array=responselist)
