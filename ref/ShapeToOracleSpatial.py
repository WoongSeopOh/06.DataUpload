# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: Shape 파일(폴더)을 읽어서 Oracle Table을 생성하고, 데이터를 입력하는 스크립트  
# Author: WoongSeopOh(wsoh@vng.kr)

# Pyshp : Shape파일만 단순히 다룰 경우 사용 // GDAL보다 가볍다.
# sf = shapefile.Reader(shp_file)
# Shape 파일 데이터 타입 확인# NULL = 0# POINT = 1# POLYLINE = 3# POLYGON = 5# MULTIPOINT = 8# POINTZ = 11# POLYLINEZ = 13# POLYGONZ = 15# MULTIPOINTZ = 18# POINTM = 21# POLYLINEM = 23# POLYGONM = 25# MULTIPOINTM = 28# MULTIPATCH = 31
# print(sf.encoding) print(sf.fields) print(sf.numRecords) print(sf.shapeType) print(sf.shapeTypeName) print(sf.shapeName) print(sf.records())

# connection = cx_Oracle.Connection("user/pw@tns")
# typeObj = connection.gettype("SDO_GEOMETRY")
# elementInfoTypeObj = connection.gettype("SDO_ELEM_INFO_ARRAY")
# ordinateTypeObj = connection.gettype("SDO_ORDINATE_ARRAY")
# obj = typeObj.newobject()
# obj.SDO_GTYPE = 2003
# obj.SDO_ELEM_INFO = elementInfoTypeObj.newobject()
# obj.SDO_ELEM_INFO.extend([1, 1003, 3])
# obj.SDO_ORDINATES = ordinateTypeObj.newobject()
# obj.SDO_ORDINATES.extend([1, 1, 5, 7])
#
# cursor = connection.cursor()
# cursor.execute("insert into TestGeometry values (1, :obj)", obj = obj)
# feature = data['features'][0]
# geom = arcgis.geometry.Geometry(feature['geometry'])
# wkb = geom.WKB
# objectid = get_next_objectid()
# cursor.setinputsizes(wkb=cx_Oracle.BLOB)
# cursor.execute("""insert into SURVEY(OBJECTID,SURVEY,SHAPE) values
# (:objectid, :survey, MDSYS.SDO_GEOMETRY(:wkb, 8265))""",
#   objectid=objectid, survey='H13195', wkb=wkb)
# ---------------------------------------------------------------------------------------------------------------------------------------------------
import datetime
import cx_Oracle
import UtilFiles
from osgeo import ogr

# 1) Shape 파일이 위치한 폴더 정의
# DATA_FOLDER = r'C:\00.Dev\99.01.CreateCmtInfo'
# DATA_FOLDER = r'D:\04.프로젝트\04.안성시\안성시 데이터(20220316)'
# DATA_FOLDER = r'D:\04.프로젝트\03.김포시(AI영상분석)\04.GTMap셋팅\원본데이터'
DATA_FOLDER = r'C:\00.Dev\03.PyPrj\00.CommonProject\03.DataConvert\CreateCmtInfo\03.LAYER'
DATA_EXT = '.shp'
ID_FLD_NM = 'FID'
SPATIAL_FLD_NM = "GEOM"

EXIST_OPTIONS = 1   # 1: Drop & Create, 2: Truncate, 3: Append
# 2) 목표 좌표계 정의
SR_ID = 5187

NOT_NEED_COLUMNS = ['FID_', 'FID', 'SHAPE_LENG', 'SHAPE_AREA', 'GEOMETRY', 'SHAPE_LEN']
DB_STRING = 'VARCHAR2'
DB_NUMBER = 'NUMBER'
DB_DATE = 'DATE'
DB_GEOMETRY = 'SDO_GEOMETRY'

GEOM_TYPE_POINT = 'MULTIPOINT'
GEOM_TYPE_POLYLINE = 'MULTILINE'
GEOM_TYPE_POLYGON = 'MULTIPOLYGON'

# db_con = cx_Oracle.connect('uis_cmt', 'uis_cmt', '192.168.0.135:1521/ORAWAVE')
db_con = cx_Oracle.connect('cmt_gp', 'cmt_gp', '192.168.0.135:1521/ORAWAVE')


# Function Declaration ------------------------------------------------------------------------------------------------------------------------------------------------------------------

# write_log------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 로그 기록용 함수
# Parameter: arg_msg(메시지본문), arg_type (ERR인 경우는 EXIT)
# Return Value:
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def write_log(arg_msg, arg_type='INFO'):
    logtime = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : "
    if arg_type == "ERR":
        print(logtime + arg_msg)
        exit()
    else:
        print(logtime + arg_msg)
    return


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: Shape파일 정보를 오라클 데이터베이스로 인서트한다.
# Parameter:arg_table(테이블명), arg_df(업로드데이터), arg_fld([fld_nm, fld_ty_nm, fld_width, fld_precision])
# Return Value:
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def insert_shp_values(arg_table, arg_layer, arg_fld, arg_geom_type):
    # 필드목록 생성
    value_list = []
    total_values = []
    fld_body_name, fld_values = '', ''
    inx = 0
    idx = 0
    for fld in arg_fld:
        inx += 1
        fld_body_name = fld_body_name + fld[0] + ','
        fld_values = fld_values + ':' + str(inx) + ','
    # 필드 수 만큼 바인드 변수 생성
    isrt_sql = "INSERT INTO " + arg_table + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"
    write_log(isrt_sql)
    # 01.CreateCmtInfo Rows (tuple 형식의 Dataframe)
    # total_size = arg_layer.GetFeatureCount()
    total_size = 0
    for feature in arg_layer:
        idx += 1
        # 정의된 필드에 맞는 정보만 리스트에 담는다.
        for fld in arg_fld:
            # FID와 GEOM은 제외
            if fld[0] not in [ID_FLD_NM, SPATIAL_FLD_NM]:
                value_list.append(feature[fld[0]])
        # 첫 요소로 아이디 추가
        value_list.insert(0, idx)
        # 마지막 요소로 Geometry 객체 추가
        geom_obj = create_geom_obj(arg_geom_type, feature.GetGeometryRef())
        value_list.append(geom_obj)
        # 전체 리스트 추가 (list Copy)
        copy_list = value_list[:]
        total_values.append(copy_list)
        if len(total_values) == 1000:
            db_cursor.executemany(isrt_sql, total_values)
            db_con.commit()
            total_size = total_size + len(total_values)
            write_log(arg_table + ' Upload Completed!!: ' + str(total_size) + ' 건')
            del total_values[:]
        del value_list[:]

    # try:
    #     # Performance
    #     # cursor.setinputsizes(None, 20)  --> 각 컬럼의 최대 크기 지정 // 숫자는 None, 컬럼마다 String의 경우 20  .. 컬럼이 다섯개면.. (1, 3, 4, 5, 6) 이런식?
    #     #
    #     # todo: Adjust the number of rows to be inserted in each iteration   # to meet your memory and performance requirements
    #     # 큰 데이터 이유로 executemany라고 해도, 나눠서 인서트 필요 (Commit, 로그 등등)
    #     start_pos = 0
    #     batch_size = 1000
    #     total_size = 0
    #     while start_pos < len(total_values):
    #         split_rows = total_values[start_pos:start_pos + batch_size]
    #         start_pos += batch_size
    #         db_cursor.executemany(isrt_sql, split_rows)
    #         db_con.commit()
    #         total_size = total_size + len(split_rows)
    #         write_log(arg_table + ' Upload Completed!!: ' + str(total_size) + ' 건')
    #
    #     # Error Handling
    #     # cursor.executemany("insert into ParentTable values (:1, :2)", data, batcherrors=True)
    #     # for error in cursor.getbatcherrors():
    #     #     print("Error", error.message, "at row offset", error.offset)
    #
    # except cx_Oracle.DatabaseError as e:
    #     write_log(isrt_sql)
    #     write_log(str(e), 'ERR')
    # finally:
    #     del value_list[:]


# create_geom_obj-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: cx_Oracle을 통해 입력할 SDE_Geomtry 객체를 생성한다.
# Parameter: arg_type(MULTIPOINT, MULTILINE, MULTIPOLYGON), feature.GetGeometryRef()
# Return Value: SDE_Geomtry 객체
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_geom_obj(arg_type, arg_geometry):
    lst_pts = []
    lst_elem_info = []
    # Geometry 객체 생성
    typeObj = db_con.gettype("MDSYS.SDO_GEOMETRY")
    pointTypeObj = db_con.gettype("MDSYS.SDO_POINT_TYPE")
    elementInfoTypeObj = db_con.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
    ordinateTypeObj = db_con.gettype("MDSYS.SDO_ORDINATE_ARRAY")

    obj = typeObj.newobject()
    # Geometry Type 정의
    if arg_type == GEOM_TYPE_POINT:
        obj.SDO_GTYPE = 2001
    elif arg_type == GEOM_TYPE_POLYLINE:
        obj.SDO_GTYPE = 2006
        lst_elem_info.extend([1, 2, 1])
    # 폴리곤인 경우
    else:
        obj.SDO_GTYPE = 2007
        lst_elem_info.extend([1, 1003, 1])

    # SR_ID 전역변수
    obj.SDO_SRID = SR_ID
    try:
        # 데이터 타입별로 처리
        # TODO: MultiPoint
        if arg_type == GEOM_TYPE_POINT:
            obj.SDO_POINT = pointTypeObj.newobject()
            obj.SDO_POINT.X = arg_geometry.Centroid().GetX()
            obj.SDO_POINT.Y = arg_geometry.Centroid().GetY()
        elif arg_type == GEOM_TYPE_POLYLINE or arg_type == GEOM_TYPE_POLYGON:
            obj.SDO_ELEM_INFO = elementInfoTypeObj.newobject()
            obj.SDO_ORDINATES = ordinateTypeObj.newobject()

            int_parts = arg_geometry.GetGeometryCount()
            # Multi인 경우
            if int_parts > 0:
                for i in range(int_parts):
                    single_geom = arg_geometry.GetGeometryRef(i)
                    # todo: 한번 더 확인해야 하는데.. 왜 인지는?
                    if single_geom.GetPoints() is None:
                        single_geom = single_geom.GetGeometryRef(0)

                    pts = single_geom.GetPoints()
                    for pt in pts:
                        # X좌표, Y좌표만
                        lst_pts.extend([pt[0], pt[1]])
                    # MultiPart 정보
                    if len(lst_pts) > 0 and i < int_parts - 1:
                        lst_elem_info.extend([len(lst_pts)+1, 2, 1])
            # Single인 경우
            else:
                pts = arg_geometry.GetPoints()
                for pt in pts:
                    # X좌표, Y좌표만
                    lst_pts.extend([pt[0], pt[1]])

            obj.SDO_ELEM_INFO.extend(lst_elem_info)
            obj.SDO_ORDINATES.extend(lst_pts)
        else:
            return None

        return obj

    except ValueError as e:
        write_log('VauleError:' + str(e))
        return None


# is_exist_table-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 데이터베이스에 테이블이 생성되어 있는지 확인하여 리턴함
# Parameter: arg_tbl_nm (테이블명)
# Return Value: True(생성), False(생성안됨)
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def is_exist_table(arg_tbl_nm):
    str_sql = "SELECT TNAME FROM TAB WHERE TNAME = '" + arg_tbl_nm + "' AND TABTYPE = 'TABLE'"
    db_cursor.execute(str_sql)
    tname = db_cursor.fetchone()
    if tname is None or tname == '':
        return False
    else:
        return True


# get_data_type -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 입력된 데이터 타입을 받아서 UIS에서 정해진 데이터타입을 리턴
# Parameter: arg_data_type (ogr geomtry type)
# Return Value: Oracle Spatial GType [MULTIPOINT, MULTILINE, MULTIPOLYGON] : UIS는 모두 MULTI 타입으로 구성함
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_geom_type(arg_data_type):
    if arg_data_type == 1 or arg_data_type == 4 or arg_data_type == 1001 or arg_data_type == 1004 or arg_data_type == 2001 or arg_data_type == 2004 or arg_data_type == 3001 or arg_data_type == 3004:
        return 'MULTIPOINT'
    elif arg_data_type == 2 or arg_data_type == 5 or arg_data_type == 1002 or arg_data_type == 1005 or arg_data_type == 2002 or arg_data_type == 2005 or arg_data_type == 3002 or arg_data_type == 3005:
        return 'MULTILINE'
    elif arg_data_type == 3 or arg_data_type == 6 or arg_data_type == 1003 or arg_data_type == 1006 or arg_data_type == 2003 or arg_data_type == 2006 or arg_data_type == 3003 or arg_data_type == 3006:
        return 'MULTIPOLYGON'
    else:
        return None


# get_db_field_type -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 입력된 데이터 타입을 받아서 DB 타입으로 변환한다.
# Parameter: arg_field_type 'Integer': 0, 'Real': 2, 'String': 4, 'Date': 9, 'Time': 10, 'DateTime': 11, 'Integer64': 12
# Return Value: field_type_string
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_db_field_type(arg_field_type):
    if arg_field_type == 0 or arg_field_type == 2 or arg_field_type == 12:
        return DB_NUMBER
    elif arg_field_type == 4:
        return DB_STRING
    elif arg_field_type == 9 or arg_field_type == 10 or arg_field_type == 11:
        return DB_DATE
    else:
        return None


# get_db_length -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 입력된 데이터 길이를 받아서 적절한 길이로 리턴한다.
# TODO: Geopanda를 이용하여 데이터의 max length를 구한후 보다 정밀하게 처리 필요 : shp_df = gpd.read_file(shp_file, encodings='utf-8')
# Parameter: arg_field_nm, arg_field_length
# Return Value: field_length
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_db_length(arg_field_nm, arg_field_length):
    if arg_field_nm == DB_STRING:
        if arg_field_length <= 16:
            return arg_field_length
        elif 16 < arg_field_length <= 32:
            return 32
        elif 32 < arg_field_length <= 64:
            return 64
        elif 64 < arg_field_length <= 128:
            return 128
        elif 128 < arg_field_length <= 256:
            return 256
        elif 256 < arg_field_length:
            return 512
        else:
            return 1024
    else:
        return arg_field_length


# get_db_scale -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 입력된 데이터 소수점 길이를 받아서 적절한 길이로 리턴한다.
# Parameter: arg_field_nm, arg_field_scale
# Return Value: field_scale
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_db_scale(arg_field_nm, arg_field_scale):
    if arg_field_nm == DB_NUMBER:
        if arg_field_scale <= 4:
            return arg_field_scale
        # TODO: 좌표인 경우 적절하게 처리하는 방안..
        elif 4 < arg_field_scale <= 7:
            return 7
        # 더 큰 수치는 없다.
        else:
            return 4
    else:
        return arg_field_scale


# create_field_list------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: Shape파일을 토대로 필드목록을 생성한다.
# Parameter: arg_table_nm, arg_field_cnt
# Return Value: rtn_sql
#     sql_header = 'INSERT INTO '
#     table_name = 'ABPM_LAND_FRST_LEDG '
#     sql_body = 'VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15)'
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_field_list(arg_layer_def):
    # 첫번째 필드 정의
    rtn_list = [[ID_FLD_NM, DB_NUMBER, 38, 0]]
    # 순서대로 필드 정의
    for i in range(arg_layer_def.GetFieldCount()):
        fld_nm = arg_layer_def.GetFieldDefn(i).GetName().upper()
        fld_ty_nm = get_db_field_type(arg_layer_def.GetFieldDefn(i).GetType())
        fld_width = get_db_length(fld_ty_nm, arg_layer_def.GetFieldDefn(i).GetWidth())
        fld_precision = get_db_scale(fld_ty_nm, arg_layer_def.GetFieldDefn(i).GetPrecision())
        # 불필요한 필드는 제거
        if fld_nm not in NOT_NEED_COLUMNS:
            # 필드별로 고정 및 예외사항인 것에 대해서 구성 TODO: 많아질 경우 상수로 관리 필요 (DICTIONARY 형태)
            if fld_nm == 'FTR_IDN':
                fld_width = 10
            # 필드별 정보 추가
            rtn_list.append([fld_nm, fld_ty_nm, fld_width, fld_precision])

    # 마지막 Geometry 필드 정의
    rtn_list.append([SPATIAL_FLD_NM, DB_GEOMETRY, 0, 0])
    return rtn_list


# create_table---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 테이블명과 필드목록을 받아서 오라클 Spatial 테이블을 생성함
# Parameter: arg_tbl_nm(테이블명), arg_fld_list(필드목록) [fld_nm, fld_ty_nm, fld_width, fld_precision], arg_type(MULTIPOINT, MULTILINE, MULTIPOLYGON)
# Return Value: None
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_layer(arg_tbl_nm, arg_fld_list, arg_type):
    is_drop_yn = False
    is_exist_yn = False
    sql_header, sql_body, sql_footer = '', '', ''
    layer_gtype = "LAYER_GTYPE=\"" + arg_type + "\""

    try:
        if is_exist_table(arg_tbl_nm):
            is_exist_yn = True
            # Drop
            if EXIST_OPTIONS == 1:
                db_cursor.execute("DROP TABLE " + arg_tbl_nm + " CASCADE CONSTRAINTS")
                db_cursor.execute("DELETE FROM MDSYS.USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '" + arg_tbl_nm + "'")
                db_con.commit()
                is_drop_yn = True
            # Truncate
            elif EXIST_OPTIONS == 2:
                db_cursor.execute("TRUNCATE TABLE " + arg_tbl_nm)

        # Create Table : 테이블이 없거나, 있는데 Drop한 경우
        if is_exist_yn is False or (is_exist_yn is True and is_drop_yn is True):
            # 필드 정의
            for fld in arg_fld_list:
                # 무조건 첫번째 위치
                if fld[0] == ID_FLD_NM:
                    sql_header = "CREATE TABLE " + arg_tbl_nm + " (FID NUMBER PRIMARY KEY,"
                # 무조건 마지막 위치
                elif fld[0] == SPATIAL_FLD_NM:
                    sql_footer = "GEOM SDO_GEOMETRY)"
                # 중간 위치
                else:
                    if fld[1] == DB_STRING:
                        sql_body = sql_body + fld[0] + " " + fld[1] + "(" + str(fld[2]) + "),"
                    elif fld[1] == DB_NUMBER:
                        sql_body = sql_body + fld[0] + " " + fld[1] + "(" + str(fld[2]) + "," + str(fld[3]) + "),"
                    elif fld[1] == DB_DATE:
                        sql_body = sql_body + fld[0] + " " + fld[1] + ","
                    else:
                        sql_body = sql_body + fld[0] + " VARCHAR2(512),"

            # Create Table Statement
            crt_sql = sql_header + sql_body + sql_footer
            write_log(crt_sql)
            db_cursor.execute(crt_sql)

            # Insert Metadata
            isrt_sql = "INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) " \
                       "VALUES (:1, :2, SDO_DIM_ARRAY(SDO_DIM_ELEMENT('X', 0, 0, 0.05),SDO_DIM_ELEMENT('Y', 0, 0, 0.05)) ,:3)"
            db_cursor.execute(isrt_sql, (arg_tbl_nm, SPATIAL_FLD_NM, SR_ID))

            # Create Index
            index_sql = "CREATE INDEX IDX_" + arg_tbl_nm + " ON " + arg_tbl_nm + "(\"" + SPATIAL_FLD_NM + "\") INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (" + "\'" + layer_gtype + "\'" + ")"
            db_cursor.execute(index_sql)
            db_con.commit()

    except cx_Oracle.DatabaseError as e:
        write_log(str(e), 'ERR')


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
db_cursor = db_con.cursor()
# 1) 특정 폴더에서 특정 확장자의 목록을 가져온다.
shp_files = UtilFiles.search_files(DATA_FOLDER, DATA_EXT)

for shp_file in shp_files:
    write_log(shp_file + "Upload Start!!" + '*' * 100)
    # Shape 기본정보 구성
    shape_source = ogr.Open(shp_file)
    if shape_source is None:
        write_log("잘못된 데이터 원본 :" + shp_file)
        continue
    shp_layer = shape_source.GetLayer(0)
    geom_type_string = get_geom_type(shp_layer.GetGeomType())
    layer_name = shp_layer.GetName().upper()
    # TODO: 좌표 spatialRef = shp_layer.GetSpatialRef() : EPSG 번호를 구해야 함

    # 필드정의 [모두 대문자로 구성]         # TODO: PostgreSQL 경우 필드명 대문자 유의...
    layer_definition = shp_layer.GetLayerDefn()
    lst_fields = create_field_list(layer_definition)
    # 레이어 생성(오라클 기준)
    create_layer(layer_name, lst_fields, geom_type_string)
    # 데이터 입력
    insert_shp_values(layer_name, shp_layer, lst_fields, geom_type_string)
    # insert_sql = create_insert_sql(layer_name, insert_df.shape[1])
    # write_log(insert_sql)
    # # 한꺼번에 처리해야 하는 데이터 건수를 제어한다. (분할해서 업로드 // 백만건 단위로)
    # start_pos = 0
    # batch_size = 1000000
    # while start_pos < len(rows):
    #     split_rows = rows[start_pos:start_pos + batch_size]
    #     start_pos += batch_size
    #     db_cursor.executemany(insert_sql, split_rows)
    #     db_con.commit()
    # write_log(layer_name + " 01.CreateCmtInfo Upload : " + str(len(rows)))

db_cursor.close()
db_con.close()

write_log("Upload Finished!!")

exit()
