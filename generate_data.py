import random

yy = open("/home/ubuntu/baby-names2.csv","r")

kk = -1

mm = []
nn = []

rr = []
ss = []

for ii in yy:
    kk += 1
    if not kk:
        continue
    jj = ii.strip().split(',')
    
    mm.append([jj[1][1:-1],jj[3]])

for ii in range(55000):
    oo = mm[random.randint(0,len(mm))]
    pp = mm[random.randint(0,len(mm))]

    qq = oo[0] + ' ' + pp[0]
    if qq not in nn:
        nn.append([qq,oo[1][1:-1]])

kk = 0
        
for ii in nn:
    if kk < 5000:
        print "Dr. " + ii[0] + ', ' + ii[1]
        rr.append(ii[0])
    else:
        print ii[0] + ', ' + ii[1]
        ss.append(ii[0])
    kk += 1

ttt = ["bellevue hospital center",
"brooklyn hospital center",
"gracie square hospital",
"harlem hospital center",
"lenox hill hospital",
"metropolitan hospital center",
"mount sinai beth israel",
"mount sinai hospital",
"mount sinai st. luke\'s",
"mount sinai west",
"new York-presbyterian/columbia university medical center",
"new York-presbytarian/lower manhanttan hospital",
"new york-presbytarian/weill cornell medical center",
"nyu langone medical center",
"bronx-lebanon hospital center",
"calvary hospital",
"jacobi medical center",
"lincoln medical center",
"montefiore medical center",
"north central bronx hospital"]

uuu = ['@insight.net','@data.com','@science.org','@engineering.gov']

tt = ["stomach ache",
      "head ache",
      "neck pain",
      "acid reflux",
      "muscle spasms",
      "depression",
      "anxiety",
      "flu",
      "heart disease",
      "psoriasis",
      "mumps",
      "cholera",
      "ebola",
      "polio",
      "toxoplasmosis",
      "measles",
      "stroke",
      "diabetes",
      "allergies",
      "arthritis",
      "asthma",
      "cold sore",
      "epilepsy",
      "gallstones",
      "gout",
      "insomnia",
      "lupus",
      "malaria",
      "obesity",
      "pneumonia"
      ]

x = open('/home/ubuntu/fakepractitioners.json','w')

for ii in range(len(rr)):

    #THIS IS THE VALUE
    a = "\"" + str(ii) + "\""
    b ="\"void\""

    #THIS IS THE PRACTITIONER
    c = "\"" + rr[ii] + "\""

    #THIS IS THE EMAIL ADDRESS
    rrr = rr[ii].split(' ')[1]
    d ="\""+ rr[ii][:1]+rrr+uuu[random.randint(0,len(uuu)-1)]+"\""

    e ="\"void\""
    f ="\"void\""
    g ="\"void\""

    #THIS IS THE HOSIPITAL
    h ="\""+ttt[random.randint(0,len(ttt)-1)]+"\""

    i ="\"void\""
    j ="\"void\""
    k ="\"void\""
    l ="\"void\""
    
    x.write("{")
    x.write("\"resourceType\" : \"Practitioner\",")
    x.write("\"identifier\" : "+ a + ", ")
    x.write("\"active\" : " + b + ",")
    x.write("\"name\" : " + c + ",")
    x.write("\"telecom\" : "+ d + ",")
    x.write("\"address\" : "+ e + ",")
    x.write("\"gender\" : " + f + ",")
    x.write("\"birthDate\" : "+ g + ",")
    x.write("\"photo\" : \"void\",")
    x.write("\"qualification\" : [{ ")
    x.write("\"identifier\" : "+ h + ",")
    x.write("\"code\" : "+ i +",")
    x.write("\"period\" : "+ j +",")
    x.write("\"issuer\" : "+ k)
    x.write(" }],")
    x.write("\"communication\" : " + l)
    x.write("}\n")

x.close()

x = open('/home/ubuntu/fakepatients.json','w')

for ii in range(len(ss)):

    #THIS IS THE VALUE
    a = "\"" + str(ii) + "\""
    
    b ="\"void\""

    #THIS IS THE PATIENT
    c = "\"" + ss[ii] + "\""

    d ="\"void\""
    e ="\"void\""
    f ="\"void\""
    g ="\"void\""
    h ="\"void\""
    i ="\"void\""
    j ="\"void\""
    k ="\"void\""
    l ="\"void\""
    m ="\"void\""
    n ="\"void\""
    o ="\"void\""
    p ="\"void\""
    q ="\"void\""
    r ="\"void\""
    s ="\"void\""
    t ="\"void\""
    u ="\"void\""
    v ="\"void\""
    w ="\"void\""
    xx ="\"void\""
    y ="\"void\""
    
    x.write("{")
    x.write("\"resourceType\" : \"Patient\",")
    x.write("\"identifier\" : "+ a + ",")
    x.write("\"active\" : "+ b +",")
    x.write("\"name\" : "+ c +",")
    x.write("\"telecom\" : " + d + ",")
    x.write("\"gender\" : " + e +",")
    x.write("\"birthDate\" : " + f + ",")
    x.write("\"deceasedBoolean\" : "+ g + ",")
    x.write("\"deceasedDateTime\" : " + h + ",")
    x.write("\"address\" : "+ i + ",")
    x.write("\"maritalStatus\" : " + j + ",")
    x.write("\"multipleBirthBoolean\" : "+ k + ",")
    x.write("\"multipleBirthInteger\" : " + l + ",")
    x.write("\"photo\" : \"void\",")
    x.write("\"contact\" : [{ ")
    x.write("\"relationship\" : "+ m + ",")
    x.write("\"name\" : " + n + ",")
    x.write("\"telecom\" : " + o + ",")
    x.write("\"address\" : " + p + ",")
    x.write("\"gender\" : " + q + ",")
    x.write("\"organization\" : " + r + ",")
    x.write("\"period\" : " + s )
    x.write(" }],")
    x.write("\"animal\" : { ")
    x.write("\"species\" : \"human\",")
    x.write("\"breed\" : \"void\",")
    x.write("\"genderStatus\" : \"void\"")
    x.write(" },")
    x.write("\"communication\" : [{ ")
    x.write("\"language\" : " + t + ",")
    x.write("\"preferred\" : " + u )
    x.write(" }],")
    x.write("\"generalPractitioner\" : " + v + ",")
    x.write("\"managingOrganization\" : " + w + ",")
    x.write("\"link\" : [{ ")
    x.write("\"other\" : " + xx +",")
    x.write("\"type\" : " + y)
    x.write(" }]")
    x.write("}\n")

x.close()

allofit = 30

for thisone in range(allofit):

    x = open('/home/ubuntu/fakedata_'+str(thisone)+'.json','w')
 
    for ii in range(6000000):

        #THIS IS THE VALUE
        a = "\"" + str(ii)  + "\""
    
        b ="\"void\""
        c ="\"void\""
        d ="\"void\""
        e ="\"void\""
        f ="\"void\""
        g ="\"void\""
        h ="\"void\""
        i ="\"void\""
        
        #THIS IS THE PATIENT
        j = "\"" + str(random.randint(0,len(ss)-1)) + "\""
    
        k ="\"void\""
    
        #THIS IS THE REFERRAL
        l ="\"void\""

        m ="\"void\""
        n ="\"void\""

        #THIS IS THE PRACTITIONER
        o = "\"" + str(random.randint(0,len(rr)-1)) + "\""
    
        p ="\"void\""
        q ="\"void\""
        r ="\"void\""
        s ="\"void\""

        #THIS IS THE DIAGNOSIS
        t = "\"" + tt[random.randint(0,len(tt)-1)] + "\""

        u ="\"void\""
        v ="\"void\""
        w ="\"void\""
        xx ="\"void\""
        y ="\"void\""
        z = "\"void\""
        aa ="\"void\""
        ab ="\"void\""
        ac ="\"void\""
        ad ="\"void\""
        ae ="\"void\""
        af ="\"void\""
        ag ="\"void\""
        ah ="\"void\""
        ai ="\"void\""
        aj ="\"void\""
        ak = "\"void\""
        
        x.write("{")
        x.write("\"resourceType\" : \"Encounter\",")
        x.write("\"identifier\" : "+ a +",")
        x.write("\"status\" : " + b  + ",")
        x.write("\"statusHistory\" : [{ ")
        x.write("\"status\" : " + c + ",")
        x.write("\"period\" : " + d)
        x.write(" }],")
        x.write("\"class\" : " + e + ",")
        x.write("\"classHistory\" : [{ ")
        x.write("\"class\" : " + f +",")
        x.write("\"period\" : " + g )
        x.write(" }],")
        x.write("\"type\" : "+ h + ",")
        x.write("\"priority\" : " + i + ",")
        x.write("\"subject\" : " + j + ",")
        x.write("\"episodeOfCare\" : " + k + ",")
        x.write("\"incomingReferral\" : " + l + ",")
        x.write("\"participant\" : [{ ")
        x.write("\"type\" : " + m + ",")
        x.write("\"period\" : " + n + ",")
        x.write("\"individual\" : " + o)
        x.write(" }],")
        x.write("\"appointment\" : " + p + ",")
        x.write("\"period\" : " + q + ",")
        x.write("\"length\" : " + r + ",")
        x.write("\"reason\" : " + s + ",")
        x.write("\"diagnosis\" : [{ ")
        x.write("\"condition\" : " + t + ",")
        x.write("\"role\" : " + u + ",")
        x.write("\"rank\" : " + v )
        x.write(" }],")
        x.write("\"account\" : " + w + ",")
        x.write("\"hospitalization\" : { ")
        x.write("\"preAdmissionIdentifier\" : " + xx + ",")
        x.write("\"origin\" : " + y + ",")
        x.write("\"admitSource\" : " + z + ",")
        x.write("\"reAdmission\" : " + aa + ",")
        x.write("\"dietPreference\" : " + ab + ",")
        x.write("\"specialCourtesy\" : " + ac + ",")
        x.write("\"specialArrangement\" : " + ad + ",")
        x.write("\"destination\" : " + ae + ",")
        x.write("\"dischargeDisposition\" : " + af)
        x.write(" },")
        x.write("\"location\" : [{ ")
        x.write("\"location\" : " + ag + ",")
        x.write("\"status\" : " + ah + ",")
        x.write("\"period\" : " + ai)
        x.write(" }],")
        x.write("\"serviceProvider\" : " + aj + ",")
        x.write("\"partOf\" : " + ak )
        x.write("}\n")
        
    x.close()
    
