import json
import random
from datetime import datetime
import flask
from flask import jsonify,request
from flask import make_response
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,TimestampType
from pyspark.sql.functions import udf,to_date,date_format
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config["JSON_AS_ASCII"] = False
spark = SparkSession.builder.getOrCreate()
def chinese_number_transfer_to_int(x):
    tr_dict={
     "三十三層":33,
    "十三層":13,
    "四層":4,
    "五層":5,
    "十二層":12,
    "十四層":14,
    "七層":7,
    "九層":9,
    "十九層":19,
    "二十一層":21,
    "二層":2,
    "十一層":11,
    "二十九層":29,
    "三層":3,
    "二十七層":27,
    "十八層":18,
    "十七層":17,
    "十五層":15,
    "二十五層":25,
    "十層":10,
    "六層":6,
    "四十二層":42,
    "一層":1,
    "十六層":16,
    "八層":8,
    "二十四層":24,
    "二十三層":23,
    "三十一層":31,
    "二十六層":26,
    "二十二層":22,
    "二十八層":28,
    "二十層":20,
    "三十八層":38,
    "三十層":30,
    "三十二層":32,
    "三十九層":39,
    "三十四層":34,
    "三十五層":35,
    "八十五層":85,
    "五十層":50,
    "三十七層":37,
    "四十一層":41,
    "四十層":40,
    "四十三層":43,
    "三十六層":36,
    "四十六層":46,   
    }
    if x in tr_dict.keys():
    
        return tr_dict[x]
chinese_number_transfer_to_int_udf = udf(lambda x:chinese_number_transfer_to_int(x),IntegerType()) 

replace_dict = {
    33:"三十三層",
    13:"十三層",
    4:"四層",
    5:"五層",
    12:"十二層",
    14:"十四層",
    7:"七層",
    9:"九層",
    19:"十九層",
    21:"二十一層",
    2:"二層",
    11:"十一層",
    29:"二十九層",
    3:"三層",
    27:"二十七層",
    18:"十八層",
    17:"十七層",
    15:"十五層",
    25:"二十五層",
    10:"十層",
    6:"六層",
    42:"四十二層",
    1:"一層",
    16:"十六層",
    8:"八層",
    24:"二十四層",
    23:"二十三層",
    31:"三十一層",
    26:"二十六層",
    22:"二十二層",
    28:"二十八層",
    20:"二十層",
    38:"三十八層",
    30:"三十層",
    32:"三十二層",
    39:"三十九層",
    34:"三十四層",
    35:"三十五層",
    85:"八十五層",
    50:"五十層",
    37:"三十七層",
    41:"四十一層",
    40:"四十層",
    43:"四十三層",
    36:"三十六層",
    46:"四十六層", 
    }
            
# prepare target csv to dataframe
a_lvr_land_a = spark.read.csv("A_lvr_land_A.csv",header=True)
b_lvr_land_a = spark.read.csv("B_lvr_land_A.csv",header=True)
e_lvr_land_a = spark.read.csv("E_lvr_land_A.csv",header=True)
f_lvr_land_a = spark.read.csv("F_lvr_land_A.csv",header=True)
h_lvr_land_a = spark.read.csv("H_lvr_land_A.csv",header=True)
# ignore english comment
a_lvr_land_a = a_lvr_land_a.filter(a_lvr_land_a.備註 != "the note")
b_lvr_land_a = b_lvr_land_a.filter(b_lvr_land_a.備註 != "the note")
e_lvr_land_a = e_lvr_land_a.filter(e_lvr_land_a.備註 != "the note")
f_lvr_land_a = f_lvr_land_a.filter(f_lvr_land_a.備註 != "the note")
h_lvr_land_a = h_lvr_land_a.filter(h_lvr_land_a.備註 != "the note")
# concat all target dataframe
df = a_lvr_land_a.union(b_lvr_land_a)
df = df.union(e_lvr_land_a)
df = df.union(f_lvr_land_a)
df = df.union(h_lvr_land_a)
# 鄉鎮市區options
taipe_area_list = a_lvr_land_a.select("鄉鎮市區").distinct().rdd.map(lambda r:r["鄉鎮市區"]).collect()
taichung_area_list = b_lvr_land_a.select("鄉鎮市區").distinct().rdd.map(lambda r:r["鄉鎮市區"]).collect()
kaohsiung_area_list = e_lvr_land_a.select("鄉鎮市區").distinct().rdd.map(lambda r:r["鄉鎮市區"]).collect()
new_taipei_area_list = f_lvr_land_a.select("鄉鎮市區").distinct().rdd.map(lambda r:r["鄉鎮市區"]).collect()
taoyuan_area_list = h_lvr_land_a.select("鄉鎮市區").distinct().rdd.map(lambda r:r["鄉鎮市區"]).collect()
area_list = df.select("鄉鎮市區").distinct().rdd.map(lambda r:r["鄉鎮市區"]).collect()
# 總樓層數options
total_floor_list = df.select("總樓層數").distinct().rdd.map(lambda r:r["總樓層數"]).collect()
# 建物型態options
building_type_list = df.select("建物型態").distinct().rdd.map(lambda r:r["建物型態"]).collect()
# Republic replace to year
def repubic_transfer(s):
    if len(s)==7:
        # debug Republic replace to year for duplicate Republic  
        if s[0:3] in s[3:8]:
            
            debug_dict ={
            "1041104":"20151104",
            "1021025":"20131025",
            "1060106":"20170106",
            "1021028":"20131028",
            "1071107":"20181107",
            "1080108":"20190108"
            }
            if s in debug_dict.keys():
                return debug_dict[s]
        else:
            return s.replace(s[0:3],str(int(s[0:3]) + 1911))        
    elif len(s)==6:
        return s.replace(s[0:2],str(int(s[0:2]) + 1911))
repubic_transfer_udf = udf(lambda s:repubic_transfer(s),StringType())
              
# print(df.select(repubic_transfer("交易年月日")).distinct().rdd.map(lambda r:r["repubic_transfer(交易年月日)"]).collect())
# repubic transfer 交易年月日 
temp_df = df.withColumn("repubic_transfer(交易年月日)",repubic_transfer_udf(df["交易年月日"]))
temp_df = temp_df.drop("交易年月日").withColumnRenamed("repubic_transfer(交易年月日)","交易年月日")
# repubic transfer 交易年月日 transfer to timestamp
def string_to_timestamp(d):
    return datetime.strptime(d,"%Y%m%d")
string_to_timestamp_udf = udf(lambda d:string_to_timestamp(d),TimestampType())
temp_df = temp_df.withColumn("string_to_timestamp(交易年月日)",string_to_timestamp_udf(temp_df["交易年月日"])).drop("交易年月日").withColumnRenamed("string_to_timestamp(交易年月日)","交易年月日")    
# # temp_df.select(to_date(temp_df['交易年月日'], 'yyyyMMdd').alias("repubic_transfer(交易年月日)_to_timestamp")).drop("交易年月日").withColumnRenamed("repubic_transfer(交易年月日)_to_timestamp","交易年月日").persist()
# sort repubic transfer 交易年月日 by timestamp
temp_df = temp_df.orderBy(temp_df["交易年月日"].desc()).persist()
# timestamp replace to string YYYY-MM-dd
def timestamp_to_string(d):
    return d.strftime("%Y-%m-%d")
timestamp_to_string_udf = udf(lambda d:timestamp_to_string(d),StringType())
temp_df = temp_df.withColumn("timestamp_to_string(交易年月日)",timestamp_to_string_udf(temp_df["交易年月日"])).drop("交易年月日").withColumnRenamed("timestamp_to_string(交易年月日)","交易年月日")        
# chinese number string transfer to int
temp_df = temp_df.withColumn("chinese_number_transfer_to_int(總樓層數)",chinese_number_transfer_to_int_udf(temp_df["總樓層數"])).drop("總樓層數").withColumnRenamed("chinese_number_transfer_to_int(總樓層數)","總樓層數")
# filter condition from exam questions
temp_df = temp_df.filter((temp_df["主要用途"]=="住家用")&(temp_df["建物型態"].startswith("住宅大樓"))&(temp_df["總樓層數"] >=13)).persist()
temp_df = temp_df.toPandas()
records = temp_df.to_dict("records")
JSON_RESPONSE_CONTENT_TYPE = "application/json;charset=UTF-8"
def _custom_response(json_string):

    response = make_response(jsonify(json_string))

    response.headers["Content-Type"] = JSON_RESPONSE_CONTENT_TYPE

    return response
a_record_list=[]
b_record_list=[]
e_record_list=[]
f_record_list=[]
h_record_list=[]
a_json = {"city":"臺北市","time_slots":a_record_list}
b_json = {"city":"臺中市","time_slots":b_record_list}
e_json = {"city":"高雄市","time_slots":e_record_list}
f_json = {"city":"新北市","time_slots":f_record_list}
h_json = {"city":"桃園市","time_slots":h_record_list}
  
@app.route('/', methods=['GET'])
def home():
    return "<h1>Hello Flask!</h1>"
@app.route('/areas', methods=['GET'])
def area_name():
    if 'area_name' in request.args:
        area_name = request.args['area_name']
        area_record_list = []
        a_area_json = {"city":"臺北市","data":area_record_list}
        b_area_json = {"city":"臺中市","data":area_record_list}
        e_area_json = {"city":"高雄市","data":area_record_list}
        f_area_json = {"city":"新北市","data":area_record_list}
        h_area_json = {"city":"桃園市","data":area_record_list}
        if area_name in taipe_area_list:
            area_df = df.filter(df["鄉鎮市區"]==area_name).persist()
            area_df = area_df.toPandas()
            area_records = area_df.to_dict("records")            
            for area_record in area_records:
                area_record_list.append(area_record)            
            return _custom_response(a_area_json)
        elif area_name in taichung_area_list:
            area_df = df.filter(df["鄉鎮市區"]==area_name).persist()
            area_df = area_df.toPandas()
            area_records = area_df.to_dict("records")
            for area_record in area_records:
                area_record_list.append(area_record)
            return _custom_response(b_area_json)
        elif area_name in kaohsiung_area_list:
            area_df = df.filter(df["鄉鎮市區"]==area_name).persist()
            area_df = area_df.toPandas()
            area_records = area_df.to_dict("records")
            for area_record in area_records:
                area_record_list.append(area_record)
            return _custom_response(e_area_json)
        elif area_name in new_taipei_area_list:
            area_df = df.filter(df["鄉鎮市區"]==area_name).persist()
            area_df = area_df.toPandas()
            area_records = area_df.to_dict("records")
            for area_record in area_records:
                area_record_list.append(area_record)
            return _custom_response(f_area_json)
        elif area_name in taoyuan_area_list:
            area_df = df.filter(df["鄉鎮市區"]==area_name).persist()
            area_df = area_df.toPandas()
            area_records = area_df.to_dict("records")
            for area_record in area_records:
                area_record_list.append(area_record)
            return _custom_response(h_area_json)  
    else:
        return "Error: No area_name provided. Please specify a area_name."

@app.route('/total_floor', methods=['GET'])
def total_floor():
    if 'total_floor' in request.args:
        a_total_floor_record_list=[]
        b_total_floor_record_list=[]
        e_total_floor_record_list=[]
        f_total_floor_record_list=[]
        h_total_floor_record_list=[]
        total_floor = request.args['total_floor']
        if int(total_floor) in replace_dict.keys():
            # use total floor int to query string by replace_dict conveting
            total_floor_df = df.filter(df["總樓層數"]==replace_dict[int(total_floor)]).persist()
            total_floor_df = total_floor_df.toPandas()
            total_floor_records = total_floor_df.to_dict("records")            
            for total_floor_record in total_floor_records:
                if total_floor_record["鄉鎮市區"] in taipe_area_list :
                    a_total_floor_record_list.append(total_floor_record)                     
                elif total_floor_record["鄉鎮市區"] in taichung_area_list :
                    b_total_floor_record_list.append(total_floor_record)                    
                elif total_floor_record["鄉鎮市區"] in kaohsiung_area_list :
                    e_total_floor_record_list.append(total_floor_record)                    
                elif total_floor_record["鄉鎮市區"] in new_taipei_area_list :
                    f_total_floor_record_list.append(total_floor_record)                    
                elif total_floor_record["鄉鎮市區"] in taoyuan_area_list :
                    h_total_floor_record_list.append(total_floor_record)                    
                a_total_floor_json = {"city":"臺北市","data":a_total_floor_record_list}
                b_total_floor_json = {"city":"臺中市","data":b_total_floor_record_list}
                e_total_floor_json = {"city":"高雄市","data":e_total_floor_record_list} 
                f_total_floor_json = {"city":"新北市","data":f_total_floor_record_list} 
                h_total_floor_json = {"city":"桃園市","data":h_total_floor_record_list}  
            total_floor_result_json = {"all_cities_data":[a_total_floor_json,b_total_floor_json,e_total_floor_json,f_total_floor_json,h_total_floor_json]}         
            return _custom_response(total_floor_result_json)
    else:
        return "Error: No total_floor provided. Please specify a total_floor."         

@app.route('/building_type', methods=['GET'])
def building_type():
    if 'building_type' in request.args:
        building_type = request.args['building_type']
        a_building_type_record_list = [] 
        b_building_type_record_list = []
        e_building_type_record_list = []
        f_building_type_record_list = []
        h_building_type_record_list = []  
        if building_type in building_type_list:
            building_type_df = df.filter(df["建物型態"]==building_type).persist()
            building_type_df = building_type_df.toPandas()
            building_type_records = building_type_df.to_dict("records")                     
            for building_type_record in building_type_records:
                if building_type_record["鄉鎮市區"] in taipe_area_list :
                    a_building_type_record_list.append(building_type_record)                 
                elif building_type_record["鄉鎮市區"] in taichung_area_list :
                    b_building_type_record_list.append(building_type_record)                    
                elif building_type_record["鄉鎮市區"] in kaohsiung_area_list :
                    e_building_type_record_list.append(building_type_record)                    
                elif building_type_record["鄉鎮市區"] in new_taipei_area_list :
                    f_building_type_record_list.append(building_type_record)                    
                elif building_type_record["鄉鎮市區"] in taoyuan_area_list :
                    h_building_type_record_list.append(building_type_record)                    
                a_building_type_json = {"city":"臺北市","data":a_building_type_record_list}
                b_building_type_json = {"city":"臺中市","data":b_building_type_record_list}
                e_building_type_json = {"city":"高雄市","data":e_building_type_record_list}
                f_building_type_json = {"city":"新北市","data":f_building_type_record_list}
                h_building_type_json = {"city":"桃園市","data":h_building_type_record_list}    
            building_type_result_json = {"all_cities_data":[a_building_type_json,b_building_type_json,e_building_type_json,f_building_type_json,h_building_type_json]}         
            return _custom_response(building_type_result_json)        

@app.route('/spark/', methods=['GET'])
def spark_result():        
    for record in records:
        if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
            a_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]}) 
        elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
            b_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
            e_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
            f_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
            h_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
    result_json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}
    return _custom_response(result_json)
@app.route('/result_to_json/', methods=['GET'])
def result_to_json():
    for record in records:
        if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
            a_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
            b_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
            e_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
            f_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
        elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
            h_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
    # random to get two cities json files        
    all_list = [a_json,b_json,e_json,f_json,h_json]        
    rand_list = []
    for i in range(1,3):
        r = random.choice(all_list)        
        jsonFile = open("result-part{}.json".format(i), "w")
        jsonFile.write(json.dumps(r,ensure_ascii=False))
        jsonFile.close()
        rand_list.append(r)

    result_json = {"all_cities_data":rand_list}
    return _custom_response(result_json)
app.run(use_reloader=False)           
