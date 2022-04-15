from datetime import datetime
import flask
from flask import jsonify,request
from flask import make_response
import pandas as pd
from pyspark.sql import SparkSession

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config["JSON_AS_ASCII"] = False
spark = SparkSession.builder.getOrCreate()
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
# RDD dataframe transfer to pandas dataframe
a_lvr_land_a = a_lvr_land_a.toPandas()
b_lvr_land_a = b_lvr_land_a.toPandas()
e_lvr_land_a = e_lvr_land_a.toPandas()
f_lvr_land_a = f_lvr_land_a.toPandas()
h_lvr_land_a = h_lvr_land_a.toPandas()
df = df.toPandas()
# show all column name and data type
# df_col_list = df.columns.values.tolist()
# for col df_col_list:
#     print(df[col])
# Republic replace to year
def repubic_transfer(s):
    if len(s)==7:
        return s.replace(s[0:3],str(int(s[0:3]) + 1911))        
    else:
        return s.replace(s[0:2],str(int(s[0:2]) + 1911))
# timestamp replace to string
def timestamp_replace_to_string(t):
    try:
        t.strftime("%Y-%m-%d")
        return t.strftime("%Y-%m-%d")
    except:
        return t 
# debug Republic replace to year
def debug_Republic_replace_to_year(b):
    debug_dict ={
            "201902019":"20190108",
            "201812018":"20181107",
            "201320135":"20131025",
            "201320138":"20131028",
            "201512015":"20151104",
        }
    if len(b)==9:
        if b in debug_dict.keys():
            return debug_dict[b]
    else:
        return b 

df["交易年月日"] = df["交易年月日"].astype(str).apply(repubic_transfer)
df["交易年月日"] = df["交易年月日"].astype(str).apply(debug_Republic_replace_to_year)
df["交易年月日"] = df["交易年月日"].fillna(value="20170106") 
df["交易年月日"] = pd.to_datetime(df["交易年月日"],format="%Y%m%d")
# sort 交易年月日 desc
df = df.sort_values(by="交易年月日",ascending=False)
df["交易年月日"] = df["交易年月日"].apply(timestamp_replace_to_string)
# 鄉鎮市區options
taipe_area_list = a_lvr_land_a["鄉鎮市區"].unique()
taichung_area_list = b_lvr_land_a["鄉鎮市區"].unique()
kaohsiung_area_list = e_lvr_land_a["鄉鎮市區"].unique()
new_taipei_area_list = f_lvr_land_a["鄉鎮市區"].unique()
taoyuan_area_list = h_lvr_land_a["鄉鎮市區"].unique()
area_list = df["鄉鎮市區"].unique()
# 建物型態options
building_type_list = df["建物型態"].unique()
# 總樓層數options
total_floor_list = df["總樓層數"].unique()
# 主要用途options
main_use_list = df["主要用途"].unique()
# Arabic numerals transfer to chinese number
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
    30:"三十層"    
}        
records = df.to_dict("records")
# solve chinese garbled
JSON_RESPONSE_CONTENT_TYPE = "application/json;charset=UTF-8"
def _custom_response(json_string):

    response = make_response(jsonify(json_string))

    response.headers["Content-Type"] = JSON_RESPONSE_CONTENT_TYPE

    return response
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
@app.route('/', methods=['GET'])
def home():
    return "<h1>Hello Flask!</h1>"

@app.route('/areas', methods=['GET'])
def area_name():
    if 'area_name' in request.args:
        area_name = request.args['area_name']
        if area_name in taipe_area_list:
            a = df[df["鄉鎮市區"]==area_name]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"臺北市","data":json_list} 
            return _custom_response(json)
        elif area_name in taichung_area_list:
            a = df[df["鄉鎮市區"]==area_name]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"臺中市","data":json_list} 
            return _custom_response(json)
        elif area_name in kaohsiung_area_list:
            a = df[df["鄉鎮市區"]==area_name]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"高雄市","data":json_list} 
            return _custom_response(json)
        elif area_name in new_taipei_area_list:
            a = df[df["鄉鎮市區"]==area_name]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"新北市","data":json_list} 
            return _custom_response(json)
        elif area_name in taoyuan_area_list:
            a = df[df["鄉鎮市區"]==area_name]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"桃園市","data":json_list}    
            return _custom_response(json)            
    else:
        return "Error: No area_name provided. Please specify a area_name."

@app.route('/total_floor', methods=['GET'])
def total_floor():
    if 'total_floor' in request.args:
        total_floor = request.args['total_floor']
        if int(total_floor) in replace_dict.keys():
            a = df[df["總樓層數"]==replace_dict[int(total_floor)]]
            records = a.to_dict("records")
            a_record_list=[]
            b_record_list=[]
            e_record_list=[]
            f_record_list=[]
            h_record_list=[]
            for record in records:
                if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
                    a_record_list.append(record)
                    a_json = {"city":"臺北市","data":a_record_list} 
                elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
                    b_record_list.append(record)
                    b_json = {"city":"臺中市","data":b_record_list}
                elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
                    e_record_list.append(record)
                    e_json = {"city":"高雄市","data":e_record_list}
                elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
                    f_record_list.append(record)
                    f_json = {"city":"新北市","data":f_record_list}
                elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
                    h_record_list.append(record)
                    h_json = {"city":"桃園市","data":h_record_list}
            json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}         
            return _custom_response(json)
    else:
        return "Error: No total_floor provided. Please specify a total_floor."        

@app.route('/building_type', methods=['GET'])
def building_type():
    if 'building_type' in request.args:
        building_type = request.args['building_type']
        if building_type in building_type_list:
            a = df[df["建物型態"]==building_type]
            records = a.to_dict("records")
            a_record_list=[]
            b_record_list=[]
            e_record_list=[]
            f_record_list=[]
            h_record_list=[]
            for record in records:
                if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
                    a_record_list.append(record)
                    a_json = {"city":"臺北市","data":a_record_list} 
                elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
                    b_record_list.append(record)
                    b_json = {"city":"臺中市","data":b_record_list}
                elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
                    e_record_list.append(record)
                    e_json = {"city":"高雄市","data":e_record_list}
                elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
                    f_record_list.append(record)
                    f_json = {"city":"新北市","data":f_record_list}
                elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
                    h_record_list.append(record)
                    h_json = {"city":"桃園市","data":h_record_list}
            json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}         
            return _custom_response(json)
        elif building_type=="住宅大樓":
            a = df[df["建物型態"].apply(lambda y:y[0:4]==building_type)]
            records = a.to_dict("records")
            a_record_list=[]
            b_record_list=[]
            e_record_list=[]
            f_record_list=[]
            h_record_list=[]
            for record in records:
                if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
                    a_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    a_json = {"city":"臺北市","time_slots":a_record_list} 
                elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
                    b_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    b_json = {"city":"臺中市","time_slots":b_record_list}
                elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
                    e_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    e_json = {"city":"高雄市","time_slots":e_record_list}
                elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
                    f_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    f_json = {"city":"新北市","time_slots":f_record_list}
                elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
                    h_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    h_json = {"city":"桃園市","time_slots":h_record_list}
            json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}         
            return _custom_response(json)       
    else:
        return "Error: No building_type provided. Please specify a building_type."

@app.route('/main_use/', methods=['GET'])
def main_use():
    if 'main_use' in request.args:
        main_use = request.args['main_use']
        if main_use in main_use_list:
            a = df[df["主要用途"]==main_use]
            records = a.to_dict("records")
            a_record_list=[]
            b_record_list=[]
            e_record_list=[]
            f_record_list=[]
            h_record_list=[]
            for record in records:
                if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
                    a_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    a_json = {"city":"臺北市","time_slots":a_record_list} 
                elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
                    b_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    b_json = {"city":"臺中市","time_slots":b_record_list}
                elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
                    e_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    e_json = {"city":"高雄市","time_slots":e_record_list}
                elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
                    f_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    f_json = {"city":"新北市","time_slots":f_record_list}
                elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
                    h_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    h_json = {"city":"桃園市","time_slots":h_record_list}
            json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}         
            return _custom_response(json)
    else:
        return "Error: No main_use provided. Please specify a main_use."        
@app.route('/total_floor_spark', methods=['GET'])
def total_floor_spark():
    if 'total_floor' in request.args:
        total_floor = request.args['total_floor']
        if total_floor =="大於等於十三層":
            df["總樓層數"] = df["總樓層數"].apply(chinese_number_transfer_to_int)
            f = df[df["總樓層數"] >=13]
            records = f.to_dict("records")
            a_record_list=[]
            b_record_list=[]
            e_record_list=[]
            f_record_list=[]
            h_record_list=[]
            for record in records:
                if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
                    a_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    a_json = {"city":"臺北市","data":a_record_list} 
                elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
                    b_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    b_json = {"city":"臺中市","data":b_record_list}
                elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
                    e_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    e_json = {"city":"高雄市","data":e_record_list}
                elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
                    f_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    f_json = {"city":"新北市","data":f_record_list}
                elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
                    h_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
                    h_json = {"city":"桃園市","data":h_record_list}
            json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}         
            return _custom_response(json)
    else:
        return "Error: No total_floor_spark provided. Please specify a total_floor_spark."

@app.route('/spark/', methods=['GET'])
def spark_result():
    df["總樓層數"] = df["總樓層數"].apply(chinese_number_transfer_to_int)
    a = df[(df["主要用途"]=="住家用")&(df["建物型態"].apply(lambda y:y[0:4]=="住宅大樓"))&(df["總樓層數"] >=13)]
    records = a.to_dict("records")
    a_record_list=[]
    b_record_list=[]
    e_record_list=[]
    f_record_list=[]
    h_record_list=[]
    for record in records:
        if record["鄉鎮市區"] in taipe_area_list and record["土地位置建物門牌"][0:3]=="臺北市":
            a_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
            a_json = {"city":"臺北市","time_slots":a_record_list} 
        elif record["鄉鎮市區"] in taichung_area_list and record["土地位置建物門牌"][0:3]=="臺中市":
            b_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
            b_json = {"city":"臺中市","time_slots":b_record_list}
        elif record["鄉鎮市區"] in kaohsiung_area_list and record["土地位置建物門牌"][0:3]=="高雄市":
            e_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
            e_json = {"city":"高雄市","time_slots":e_record_list}
        elif record["鄉鎮市區"] in new_taipei_area_list and record["土地位置建物門牌"][0:3]=="新北市":
            f_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
            f_json = {"city":"新北市","time_slots":f_record_list}
        elif record["鄉鎮市區"] in taoyuan_area_list and record["土地位置建物門牌"][0:3]=="桃園市":
            h_record_list.append({"date":record["交易年月日"],"events":[{"district":record["鄉鎮市區"],"building_state":record["建物型態"]}]})
            h_json = {"city":"桃園市","time_slots":h_record_list}
    json = {"all_cities_data":[a_json,b_json,e_json,f_json,h_json]}         
    return _custom_response(json)           
app.run(use_reloader=False)