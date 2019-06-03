'''
COMP9321 2019 Term 1 Assignment Two Code Template
Student Name: Rishabh Khurana
Student ID: z5220427
'''
import sqlite3
import datetime
import requests
import json
from flask import Flask
from flask_restplus import Resource, Api, fields, reqparse

app = Flask(__name__)
api = Api(app)

db_file_name='data.db'

indicator = api.model('indicator',{'indicator_id':fields.String('NY.GDP.MKTP.CD')})
ns=api.namespace("collections",description="Operations related to collections of data")


def create_db(db_file):
    conn=sqlite3.connect(db_file)
    c=conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS collections (collection_id varchar(20)
    ,indicator varchar(20),
    indicator_value varchar(20),
    creation_time varchar(20),
    country varchar(20),
    date varchar(20),
    value real(20))""")
    conn.commit()
    return conn

def get_json(url):
    resp = requests.get(url=url)
    data = resp.json()
    return data

@ns.route('/')
class Collections(Resource):
    @api.expect(indicator)
    @api.response(200, 'OK')
    @api.response(201, 'Created')
    @api.response(404, 'Indicator is not Valid')
    @api.doc(description="Import all collections from source")
    def post(self):
        # Url for data which is contained in two pages
        finalUrl="http://api.worldbank.org/v2/countries/all/indicators/"+api.payload["indicator_id"]+"?date=2013:2018&format=json&per_page=100"
        resp_body=dict(
            location = "EMPTY", 
            collection_id = "EMPTY",  
            creation_time= "EMPTY",
            indicator = "EMPTY"
        )

        if 'message' in get_json(finalUrl)[0]:
            return {"message":"Invalid value of indicator_id"}, 404

        # data loaded for page one
        page_data=get_json(finalUrl)[1]

        #check if data exists
        conn=create_db(db_file_name)
        c=conn.cursor()
        res=c.execute("select distinct collection_id,creation_time from collections where collection_id=?",[api.payload['indicator_id']])
        res=list(res)

        if(len(res)!=0):
            resp_body['location']="/collections/"+res[0][0]
            resp_body['collection_id']=res[0][0]
            resp_body['indicator']=res[0][0]
            resp_body['creation_time']=res[0][1]
            conn.close()
            return resp_body,200
        else:
            resp_body['location']="/collections/"+api.payload["indicator_id"]
            resp_body['collection_id']=api.payload["indicator_id"]
            resp_body['indicator']=api.payload["indicator_id"]
            dateVal=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            resp_body['creation_time']=dateVal
            resp_body=json.dumps(resp_body)
            loaded_r = json.loads(resp_body)
            #load data to database
            for data in page_data:
                c.execute("insert into collections values (?, ?, ?, ?, ?, ?, ?)",[data['indicator']['id'],data['indicator']['id'],data['indicator']['value'],dateVal,data['country']['value'],data['date'],data['value']])
            conn.commit()
            conn.close()
            return loaded_r,201

    @api.response(200, 'OK')
    @api.response(404, 'No data available')
    @api.doc(description="Get all available collections from database")
    def get(self):
        resp_list=[]
        resp_body=dict(
            location = "/<collections>/<collection_id_1>", 
            collection_id = "collection_id_1",  
            creation_time = "<time>",
            indicator = "<indicator>"
        )
        conn=create_db(db_file_name)
        c=conn.cursor()
        # gives out all the  distinct collection values and the respective time
        res=c.execute("select distinct collection_id,creation_time from collections")
        res=list(res)
        if (len(res) < 1):
            conn.close()
            return {'message':'No data available'},404
        else:
            for coll_id,date_time in res:
                resp_body['location']="/collections/"+coll_id
                resp_body['collection_id']=coll_id
                resp_body['creation_time']=date_time
                resp_body['indicator']=coll_id
                resp_list.append(resp_body)
            conn.commit()
            conn.close()
            return resp_list,200

@ns.route('/<collection_id>')
class CollectionsID(Resource):

    @api.response(200, 'OK')
    @api.response(404, 'Data not availables')
    @api.doc(description="delete a collection from database")
    def delete(self,collection_id):
        resp_body=dict(
            message ="Collection = <collection_id> is removed from the database!"
        )
        resp_body['message']="Collection = "+collection_id+" is removed from the database!"
        conn=create_db(db_file_name)
        c=conn.cursor()
        res=c.execute("select distinct collection_id from collections where collection_id=?",[collection_id])
        res=list(res)
        if(len(res)<1):
            conn.close()
            return {'message':'No data available with given collection_id'},404
        else:
            # delete user specified data from database 
            c.execute("DELETE FROM collections WHERE collection_id LIKE ?",[collection_id])
            conn.commit()
            conn.close()
            return resp_body,200

    @api.response(404, 'Data not availables')
    @api.response(200, 'OK')
    @api.doc(description="Get data for a specific collection stored in database")
    def get(self,collection_id):
        country_arr=[]
        resp_body=dict(  
            collection_id = "EMPTY",
            indicator= "EMPTY",
            indicator_value= "EMPTY",
            creation_time = "EMPTY",
            entries = []    
        )
        conn=create_db(db_file_name)
        c=conn.cursor()
        # to save the collection_id data in response
        res=c.execute("select distinct collection_id,indicator_value,creation_time from collections where collection_id LIKE ?",[collection_id])
        res=list(res)
        if(len(res)<1):
            conn.close()
            return {'message':'No data available with given collection_id'},404
        else:
            resp_body['collection_id']=res[0][0]
            resp_body['indicator']=res[0][0]
            resp_body['indicator_value']=res[0][1]
            resp_body['creation_time']=res[0][2]
            res_two=c.execute("select country,value,date from collections where collection_id=?",[collection_id])
            # adding country data to array
            for c,v,d in res_two:
                resp_country=dict(
                    country="country",
                    date="date",
                    value="value"
                )
                resp_country['country']=c
                resp_country['date']=d
                resp_country['value']=v
                country_arr.append(resp_country)

            resp_body['entries']=country_arr
            conn.close()
            return resp_body

@ns.route('/<collection_id>/<year>/<country>')
class CollectionQuery(Resource):
    @api.response(404, 'Data not availables')
    @api.response(200, 'OK')
    @api.doc(description="Get data for a specific collection stored in database by giving input country and year")
    def get(self,collection_id,year,country):
        resp_body=dict(
            collection_id= "EMPTY",
            indicator = "EMPTY",
            country= "EMPTY", 
            year= "EMPTY",
            value= "EMPTY"
        )
        # retrive rquired data from database
        conn=create_db(db_file_name)
        c=conn.cursor()
        res=c.execute("""select distinct collection_id,country,date,indicator_value
        from collections where collection_id=? 
        and country=? and date=?""",[collection_id,country,year])
        res=list(res)
        if(len(res)<1):
            conn.close()
            return {'message':'No data available with given input'},404
        else:
            # add data to response dict retrieved from the database
            resp_body['collection_id']=res[0][0]
            resp_body['indicator']=res[0][0]
            resp_body['country']=res[0][1]
            resp_body['year']=res[0][2]
            resp_body['value']=res[0][3]
            conn.close()
            return resp_body


val_args=reqparse.RequestParser()
val_args.add_argument('q',type=str,help="Top<N> or Bottom<N> values from the list")
@ns.route('/<collection_id>/<year>')
class CollectionParam(Resource):
    @api.response(200, 'OK')
    @api.response(404, 'Data not available')
    @api.doc(description="Get data for a specific collection stored in database by year and query")
    @api.expect(val_args, validate=True)
    def get(self,collection_id,year):
        resp_body=dict(
            indicator= "EMPTY",
            indicator_value= "EMPTY",
            entries=[]
        )
        country_arr=[]
        conn=create_db(db_file_name)
        c=conn.cursor()
        res=c.execute("select distinct indicator,indicator_value from collections where collection_id=? and date=?",[collection_id,year])
        res=list(res)
        if(len(res)<1):
            conn.close()
            return {'message':'No data available with given input'},404
        else:
            # update response data
            resp_body['indicator']=res[0][0]
            resp_body['indicator_value']=res[0][1]
            # extract entries
            res_two=c.execute("""select country, date, value from collections 
            where collection_id=? and date=?""",[collection_id,year])
            for i in res_two:
                resp_ent=dict(
                    country=i[0],
                    date=i[1],
                    value=i[2]
                )
                country_arr.append(resp_ent)
            q=val_args.parse_args()['q']
            if "top" in q:
                N=int(q[3:])
                country_arr=sorted(country_arr, key = lambda x: x['value'],reverse=True) 
                country_arr=country_arr[:N]
            else:
                N=int(q[6:])
                country_arr=sorted(country_arr, key = lambda x: x['value'])
                country_arr=country_arr[:N]
            resp_body['entries']=country_arr
            conn.close()
            return resp_body

if __name__ == '__main__':
    #create_db('data.db')
    app.run(debug=True)
