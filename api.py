import flask
from flask import jsonify,request
from flask import make_response
import pandas as pd

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config["JSON_AS_ASCII"] = False
# prepare target csv to dataframe
a_lvr_land_a = pd.read_csv("A_lvr_land_A.csv")
# drop english comment
a_lvr_land_a = a_lvr_land_a.drop(0)
# show all column name and data type
# a_lvr_land_a_col_list = a_lvr_land_a.columns.values.tolist()
# for col in a_lvr_land_a_col_list:
#     print(a_lvr_land_a[col])
# Republic replace to year
a_lvr_land_a["交易年月日"] = a_lvr_land_a["交易年月日"].astype(str).apply(lambda s:s.replace(s[0:3],str(int(s[0:3]) + 1911)) if len(s) > 6 else s.replace(s[0:2],str(int(s[0:2]) + 1911)))
# fix Irregular to regular
a_lvr_land_a["交易年月日"] = a_lvr_land_a["交易年月日"].astype(str).apply(lambda s:s[0:6]+s[7:9] if len(s) == 9 else s)
a_lvr_land_a["交易年月日"] = pd.to_datetime(a_lvr_land_a["交易年月日"],format="%Y%m%d")
a_lvr_land_a["交易年月日"] = a_lvr_land_a["交易年月日"].dt.strftime("%Y-%m-%d")
# 鄉鎮市區options
area_list = a_lvr_land_a["鄉鎮市區"].unique()
# 建物型態options
building_type_list = a_lvr_land_a["建物型態"].unique()
# 總樓層數options
total_floor_list = a_lvr_land_a["總樓層數"].unique()
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
records = a_lvr_land_a.to_dict("records")
# solve chinese garbled
JSON_RESPONSE_CONTENT_TYPE = "application/json;charset=UTF-8"
def _custom_response(json_string):

    response = make_response(jsonify(json_string))

    response.headers["Content-Type"] = JSON_RESPONSE_CONTENT_TYPE

    return response
@app.route('/', methods=['GET'])
def home():
    return "<h1>Hello Flask!</h1>"

@app.route('/city/all', methods=['GET'])
def city_all():
    json_list = []
    for record in records:
        json_list.append(record)
    json = {"city":"台北市","data":json_list} 
    return _custom_response(json)

@app.route('/areas', methods=['GET'])
def area_name():
    if 'area_name' in request.args:
        area_name = request.args['area_name']
        if area_name in area_list:
            a = a_lvr_land_a[a_lvr_land_a["鄉鎮市區"]==area_name]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"台北市","data":json_list} 
            return _custom_response(json)
    else:
        return "Error: No area_name provided. Please specify a area_name."

@app.route('/total_floor', methods=['GET'])
def total_floor():
    if 'total_floor' in request.args:
        total_floor = request.args['total_floor']
        if int(total_floor) in replace_dict.keys():
            a = a_lvr_land_a[a_lvr_land_a["總樓層數"]==replace_dict[int(total_floor)]]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"台北市","data":json_list} 
            return _custom_response(json)
    else:
        return "Error: No total_floor provided. Please specify a total_floor."        

@app.route('/building_type', methods=['GET'])
def building_type():
    if 'building_type' in request.args:
        building_type = request.args['building_type']
        if building_type in building_type_list:
            a = a_lvr_land_a[a_lvr_land_a["建物型態"]==building_type]
            records = a.to_dict("records")
            json_list = []
            for record in records:
                json_list.append(record)
            json = {"city":"台北市","data":json_list} 
            return _custom_response(json)
    else:
        return "Error: No building_type provided. Please specify a building_type."            

app.run()