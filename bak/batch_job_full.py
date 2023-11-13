
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램
# 전체 데이터를 업로드하고, 기존 데이터를 히스토리 테이블로 옮기는 데이터 유형 처리
# ---------------------------------------------------------------------------------------------------------------------------------------------------
import re
import sys
from datetime import datetime
import geopandas as gpd
import gc
import os
import cx_Oracle

import constant
import meta_info
import log
import utils
import config

SR_ID = 2097
DB_GEOMETRY = 'SDO_GEOMETRY'
GEOM_TYPE_POINT = 'MULTIPOINT'
GEOM_TYPE_POLYLINE = 'MULTILINE'
GEOM_TYPE_POLYGON = 'MULTIPOLYGON'

# DB Connect
db_con = cx_Oracle.connect(user=config.db_user, password=config.db_pass, dsn=config.db_dsn)
db_cur = db_con.cursor()
# Geometry 객체 생성
typeObj = db_con.gettype("MDSYS.SDO_GEOMETRY")
elementInfoTypeObj = db_con.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
ordinateTypeObj = db_con.gettype("MDSYS.SDO_ORDINATE_ARRAY")

logger = log.logger
upload_values = list()

execute_day = str(datetime.now().strftime("%Y%m%d"))

# 오라클 nls_lang : AMERICAN_AMERICA.KO16MSWIN949
# os.environ['NLS_LANG'] = ".AL32UTF8"
os.environ['NLS_LANG'] = "AMERICAN_AMERICA.KO16MSWIN949"


# db_log-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def db_log(arg_data_nm, prcs_type, arg_file_date, err_msg=None):
    cursor = db_con.cursor()

    try:
        # prcs_type : 1) 시작, 2) 종료, 3) 에러
        if prcs_type == '1':
            u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET LAST_DAILY_UPDATE = :1, BGN_BATCH_DATE = SYSDATE, LAST_UPDATE_DATE = SYSDATE, END_BATCH_DATE = NULL, SCSS_YN = NULL, RMK = NULL WHERE DATA_NM = :2"
            cursor.execute(u_sql, [arg_file_date, arg_data_nm])
        elif prcs_type == '2':
            u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :1"
            cursor.execute(u_sql, [arg_data_nm])
        else:
            u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'N', LAST_UPDATE_DATE = SYSDATE, RMK = :1 WHERE DATA_NM = :2"
            cursor.execute(u_sql, [err_msg, arg_data_nm])

        cursor.close()
        db_con.commit()

    except cx_Oracle.DatabaseError as err:
        logger.error(f"Error occured!!: {str(err)}")
        cursor.close()
        db_con.close()
        sys.exit()


# create_geom_obj-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_geom_obj(geom_type, geom):
    lst_pts = []
    lst_elem_info = []
    type_var = None

    obj = typeObj.newobject()

    # Geometry Type 정의
    if geom_type == 'Point':
        obj.SDO_GTYPE = 2001
    elif geom_type == 'MultiPoint':
        obj.SDO_GTYPE = 2005
    elif geom_type == 'LineString':
        obj.SDO_GTYPE = 2002
        type_var = 2
        lst_elem_info.extend([1, 2, 1])
    elif geom_type == 'MultiLineString':
        obj.SDO_GTYPE = 2006
        type_var = 2
        lst_elem_info.extend([1, 2, 1])
    elif geom_type == 'Polygon':
        obj.SDO_GTYPE = 2003
        type_var = 1003
        lst_elem_info.extend([1, 1003, 1])
    elif geom_type == 'MultiPolygon':
        obj.SDO_GTYPE = 2007
        type_var = 1003
        lst_elem_info.extend([1, 1003, 1])

    # SR_ID 전역변수
    obj.SDO_SRID = SR_ID
    if geom_type == 'Point':
        pointTypeObj = db_con.gettype("MDSYS.SDO_POINT_TYPE")
        obj.SDO_POINT = pointTypeObj.newobject()
        obj.SDO_POINT.X = geom.Centroid().GetX()
        obj.SDO_POINT.Y = geom.Centroid().GetY()

    elif geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']:
        obj.SDO_ELEM_INFO = elementInfoTypeObj.newobject()
        obj.SDO_ORDINATES = ordinateTypeObj.newobject()

        if geom_type in ['LineString', 'Polygon']:
            for pt in geom.exterior.coords:
                lst_pts.extend([pt[0], pt[1]])

        if geom_type in ['MultiLineString', 'MultiPolygon']:
            int_parts = len(geom.geoms)
            i = 0
            for g in geom.geoms:
                for pt in g.exterior.coords:
                    lst_pts.extend([pt[0], pt[1]])
                if i < int_parts - 1:
                    lst_elem_info.extend([len(lst_pts)+1, type_var, 1])
                i += 1

        obj.SDO_ELEM_INFO.extend(lst_elem_info)
        obj.SDO_ORDINATES.extend(lst_pts)
    else:
        return None

    return obj


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def insert_shp_values(layer_nm, dic_info, lyr_df, lyr_full_nm, db):
    value_list = []
    total_values = []
    cursor = db.cursor()

    # 필드에 해당하는 인서트 구문 생성
    dict_fields = dic_info.get('fields')
    fld_body_name, fld_values = '', ''
    inx = 0
    idx = 1
    for fld in dict_fields.keys():
        inx += 1
        fld_body_name = fld_body_name + fld + ','
        fld_values = fld_values + ':' + str(inx) + ','

    # 필드 수 만큼 바인드 변수 생성
    isrt_sql = "INSERT INTO " + layer_nm + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"
    int_test = 1
    for index, row in lyr_df.iterrows():
        for fld in dict_fields.keys():
            # TODO: 테이블마다 공간정보 필드 이름이 다르다.. OR 처리 필요..
            if fld == 'SHAPE':
                g_type = row.geometry.geom_type
                g_geom = row.geometry
                geom = create_geom_obj(g_type, g_geom)
                value_list.append(geom)
            else:
                if dict_fields.get(fld) in row.keys():
                    if utils.isNaN(row.get(dict_fields.get(fld))):
                        value_list.append(None)
                    else:
                        value_list.append(row.get(dict_fields.get(fld)))
                else:
                    value_list.append(idx)
                    idx += 1
        copy_list = value_list[:]
        total_values.append(copy_list)
        del value_list[:]
        int_test += 1

    try:
        # Performance
        # cursor.setinputsizes(None, 20)  --> 각 컬럼의 최대 크기 지정 // 숫자는 None, 컬럼마다 String의 경우 20  .. 컬럼이 다섯개면.. (1, 3, 4, 5, 6) 이런식?
        # 큰 데이터 이유로 executemany라고 해도, 나눠서 인서트 필요 (Commit, 로그 등등)
        start_pos = 0
        batch_size = 10000
        total_size = 0
        while start_pos < len(total_values):
            split_rows = total_values[start_pos:start_pos + batch_size]
            start_pos += batch_size
            cursor.executemany(isrt_sql, split_rows)
            db.commit()
            total_size = total_size + len(split_rows)
        logger.info(f"{lyr_full_nm} Upload Completed!!: {str(total_size)} 건")

    except cx_Oracle.DatabaseError as err:
        logger.error(isrt_sql)
        logger.error(f"Error occured!!: {str(err)}")
    finally:
        cursor.close()
        del value_list[:]


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
logger.info('start all_batch_job!!')

# 1) 마스터 정보 DB에서 읽어오기DATA_NM VARCHAR2(64 CHAR),
lst_batch_mng = list()
center_ling_gpd = None

try:
    # 데이터명, 전체처리여부, 레이어여부, 마지막배치일자
    strSQL = "SELECT DATA_NM, ALL_BATCH_YN, LAYER_YN, BGN_BATCH_DATE, END_BATCH_DATE FROM T_LNDB_L_BATCH_MNG WHERE USE_YN = 'Y'"
    db_cur.execute(strSQL)
    for rec in db_cur:
        lst_batch_mng.append(rec)
    db_cur.close()

except cx_Oracle.DatabaseError as e:
    logger.error(f"Database Error Occured: {str(e)}")
    db_cur.close()
    db_con.close()
    sys.exit()

# 2) 작업폴더 생성
utils.create_job_folder(constant.target_folder_path + execute_day)

# 데이터별로 처리
for data in lst_batch_mng:
    data_nm = data[0]
    all_batch_yn = data[1]
    layer_yn = data[2]
    # for debug
    # if data_nm != 'LARD_ADM_SECT_SGG':
    #     continue
    logger.info(f"{data_nm}: Database Processing Startd!! --------------------------------------------------------------------------------------------------------")
    # NS센터에서 전송된 원본 압축파일 위치
    try:
        dict_inifo = meta_info.layer_info.get(data_nm)
        source_folder_nm = constant.source_folder_path + data_nm
        # 압축파일이 풀릴 위치
        target_folder_nm = constant.target_folder_path + execute_day + '/' + data_nm
        utils.create_job_folder(target_folder_nm)
        utils.create_job_folder(target_folder_nm + '/unzip')

        unzip_folder = utils.get_folder_move_and_unzip(source_folder_nm, target_folder_nm, data_nm)
        if unzip_folder is None:
            continue
        logger.info(f"{unzip_folder}: Unzip and data moving finished!!")

        # 17개 시도 데이터가 압축풀려서 한 폴더에 위치함.
        lst_files = os.listdir(unzip_folder)

        # 파일날짜 가져오기 re.sub(r'[^0-9]','',a[23:])
        if len(lst_files) > 0:
            int_pos = meta_info.file_date_pos.get(data_nm)
            file_date = re.sub(r'[^0-9]', '', lst_files[0][int_pos:])
        else:
            file_date = None
        db_log(data_nm, '1', file_date, None)

        if layer_yn == 'Y':
            for u_file in lst_files:
                if u_file[-4:] == '.shp':
                    # full name
                    shp_full_nm = os.path.join(unzip_folder, u_file)
                    logger.info(shp_full_nm)
                    shp_df = gpd.read_file(shp_full_nm, encoding='euckr')
                    if shp_df is None:
                        logger.error(f"Wrong Shape File: {str(shp_full_nm)}")
                        continue
                    # 지적도 클립 작업 진행
                    if u_file.startswith('LSMD_CONT_LDREG'):
                        # Centerline 가져오기
                        if center_ling_gpd is None:
                            center_ling_gpd = gpd.read_file(constant.center_line_shp, encoding='euckr')
                            logger.info(center_ling_gpd.shape)
                        shp_df = gpd.clip(shp_df, center_ling_gpd)
                        logger.info(shp_df.shape)

                    # Insert Data
                    insert_shp_values(dict_inifo['table'], dict_inifo, shp_df, shp_full_nm, db_con)

                    # 메모리 해제
                    del [[shp_df]]
                    gc.collect()
                    shp_df = gpd.GeoDataFrame()
        else:
            # sqlldr
            for u_file in lst_files:
                # LSCT_LAWDCD 법정동코드는  .csv 파일임
                if u_file[-4:] == '.txt' or u_file[-4:] == '.csv':
                    ctl_path = f"{constant.sqlldr_ctr_folder}{data_nm}.ctl"
                    log_path = f"{constant.sqlldr_log_folder}/{data_nm}_{execute_day}.log"
                    bad_path = f"{constant.sqlldr_bad_folder}/{data_nm}_{execute_day}.bad"
                    data_path = f"{unzip_folder}/{u_file}"
                    command = rf"sqlldr userid={config.db_user}/{config.db_pass}@{config.db_dsn} control={ctl_path} data={data_path} log={log_path} bad={bad_path} rows=100000 direct=TRUE"
                    os.system(command)
                    logger.info(f"{unzip_folder}/{u_file}: Sqlldr Success!!")
        # 로그
        db_log(data_nm, '2', file_date, None)
    except Exception as e:
        logger.error(f"Error occured!!: {str(e)}")
        db_log(data_nm, '3', None, str(e))


logger.info('finish batch job!!')
