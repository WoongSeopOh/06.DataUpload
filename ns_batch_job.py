
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램 (일변경분만 처리)
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
from config import config

SR_ID = 2097
DB_GEOMETRY = 'SDO_GEOMETRY'
GEOM_TYPE_POINT = 'MULTIPOINT'
GEOM_TYPE_POLYLINE = 'MULTILINE'
GEOM_TYPE_POLYGON = 'MULTIPOLYGON'

# DB Connect
db_con = cx_Oracle.connect(user=config.db_user, password=config.db_pass, dsn=config.db_dsn)

# Geometry 객체 생성
typeObj = db_con.gettype("MDSYS.SDO_GEOMETRY")
elementInfoTypeObj = db_con.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
ordinateTypeObj = db_con.gettype("MDSYS.SDO_ORDINATE_ARRAY")

logger = log.logger
execute_day = str(datetime.now().strftime("%Y%m%d"))

# 오라클 nls_lang : AMERICAN_AMERICA.KO16MSWIN949
# os.environ['NLS_LANG'] = ".AL32UTF8"
os.environ['NLS_LANG'] = "AMERICAN_AMERICA.KO16MSWIN949"


# get_sqlldr_nm ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_sqlldr_nm(arg_date, arg_nm):

    if arg_nm == constant.LSCT_LAWDCD:
        return arg_nm
    elif arg_nm == constant.ABPD_UNQ_NO_CHG_HIST:
        return arg_nm + 'C'
    else:
        if len(arg_date) == 8:
            return arg_nm + '_C'
        else:
            return arg_nm


# truncate_table ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def truncate_table(arg_data_nm):
    cursor = db_con.cursor()

    try:
        truncate_sql = "TRUNCATE TABLE GIS_MAIN." + arg_data_nm
        cursor.execute(truncate_sql)
        cursor.close()

    except cx_Oracle.DatabaseError as err:
        logger.error(f"error occured!!: {str(err)}")
        cursor.close()
        db_con.close()
        sys.exit()


# is_processing ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def is_processing(opt, date_value, arg_month, arg_date, arg_data_nm):
    # 예외케이스 정의
    if arg_data_nm == constant.LSCT_LAWDCD:
        return True

    if opt == 'full' and date_value is not None and len(date_value) == 6:
        if date_value <= arg_month:
            logger.info(f"{unzip_folder}/{u_file}: T_LNDB_BATCH_MNG 테이블의 최종 업데이트 월보다 이전 정보는 업데이트할 수 없습니다.")
            return False
    else:
        if date_value is not None and len(date_value) == 8:
            if date_value <= arg_date:
                logger.info(f"{unzip_folder}/{u_file}: T_LNDB_BATCH_MNG 테이블의 최종 업데이트 일자보다 이전 정보는 업데이트할 수 없습니다.")
                return False
        else:
            return False

    return True


# get_parameters ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_parameters(argvs):
    rtn_date = None
    lst_data = None

    if len(argvs) > 1:
        # daily or full
        rtn_opt = argvs[1]
        if rtn_opt is None:
            print("error - set first parameter option: daily | full")
            sys.exit()

        if len(argvs) > 2:
            # 날짜
            rtn_date = argvs[2]
            if rtn_date is not None:
                if rtn_date.upper() == 'SKIP':
                    rtn_date = None
                    # if not utils.validate_date(rtn_date):
                    #     print(f"error - set second parameter option: Incorrect data format({rtn_date})")
                    #     sys.exit()

            if len(argvs) > 3:
                # 특정 데이터만
                rtn_data = argvs[3]
                if rtn_data is not None:
                    lst_data = rtn_data.split('|')
    else:
        print(f"error - set parameter options")
        sys.exit()

    return rtn_opt, rtn_date, lst_data


# get_batch_master ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_batch_master(arg_param):
    cursor = db_con.cursor()

    rtn_list = list()
    try:
        if arg_param == 'full':
            # 전체 지우고 다시올리기 (단, 지적도는 제외)
            strSQL = "SELECT DATA_NM, LAYER_YN, LAST_DAILY_UPDATE, LAST_FULL_UPDATE FROM T_LNDB_BATCH_MNG WHERE USE_YN = 'Y' AND ALL_BATCH_YN = 'Y'"
        else:
            # 일변경 데이터 처리: 데이터명, 전체처리여부, 레이어여부, 마지막배치일자
            strSQL = "SELECT DATA_NM, LAYER_YN, LAST_DAILY_UPDATE, LAST_FULL_UPDATE FROM T_LNDB_BATCH_MNG WHERE USE_YN = 'Y' AND DAY_YN = 'Y'"
        cursor.execute(strSQL)
        for rec in cursor:
            rtn_list.append(rec)
        cursor.close()

    except cx_Oracle.DatabaseError as e:
        logger.error(f"Database Error Occured: {str(e)}")
        cursor.close()
        db_con.close()
        sys.exit()

    return rtn_list


# db_log----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def db_log(arg_data_nm, prcs_type, arg_file_date, err_msg=None):
    cursor = db_con.cursor()

    try:
        # prcs_type : 1) 시작, 2) 변동분, 3) 전체분, 4) 에러
        if prcs_type == 'start':
            u_sql = "UPDATE T_LNDB_BATCH_MNG SET BGN_BATCH_DATE = SYSDATE, LAST_UPDATE_DATE = SYSDATE, END_BATCH_DATE = NULL, SCSS_YN = NULL, RMK = NULL WHERE DATA_NM = :1"
            cursor.execute(u_sql, [arg_data_nm])
        elif prcs_type == 'daily':
            if arg_file_date is not None:
                u_sql = "UPDATE T_LNDB_BATCH_MNG SET LAST_DAILY_UPDATE = :1, END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :2"
                cursor.execute(u_sql, [arg_file_date, arg_data_nm])
            else:
                u_sql = "UPDATE T_LNDB_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :1"
                cursor.execute(u_sql, [arg_data_nm])
        elif prcs_type == 'full':
            if arg_file_date is not None:
                u_sql = "UPDATE T_LNDB_BATCH_MNG SET LAST_FULL_UPDATE = :1, END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :2"
                cursor.execute(u_sql, [arg_file_date, arg_data_nm])
            else:
                u_sql = "UPDATE T_LNDB_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :1"
                cursor.execute(u_sql, [arg_data_nm])
        elif prcs_type == 'error':
            u_sql = "UPDATE T_LNDB_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'N', LAST_UPDATE_DATE = SYSDATE, RMK = :1 WHERE DATA_NM = :2"
            cursor.execute(u_sql, [err_msg, arg_data_nm])

        cursor.close()
        db_con.commit()

    except cx_Oracle.DatabaseError as err:
        logger.error(f"error occured!!: {str(err)}")
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
        logger.error(f"error occured!!: {str(err)}")
    finally:
        cursor.close()
        del value_list[:]


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    dict_data = dict()
    logger.info('start batch_job_day!!')
    # 파라미터 셋팅 (변동분처리인지, 전체인지) / 특정날짜값이 있는지..
    batch_opt, batch_date, lst_dataset = get_parameters(sys.argv)
    logger.info(f"parameters : {batch_opt}, {batch_date}, {lst_dataset}")

    # 1) 마스터 정보 DB에서 읽어오기DATA_NM VARCHAR2(64 CHAR),
    lst_prcs_date = list()
    lst_batch_mng = get_batch_master(batch_opt)

    # 2) 작업폴더 생성
    utils.create_job_folder(constant.target_folder_path + execute_day)

    # 데이터별로 처리
    for data in lst_batch_mng:
        data_nm = data[0]
        layer_yn = data[1]
        std_date = data[2]
        std_month = data[3]

        # for debug
        # if data_nm != 'LARD_ADM_SECT_SGG':
        #     continue

        # 실행파라미터에 데이터가 명시되었을 경우 해당 데이터만 처리함.
        if lst_dataset is None or (lst_dataset is not None and data_nm in lst_dataset):
            dict_data[data_nm] = list()
            logger.info(f"{data_nm}: NS Data Upload Processing Startd!! -----------------------------------")
            # NS센터에서 전송된 원본 압축파일 위치
            try:
                int_pos = meta_info.file_date_pos.get(data_nm)
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
                db_log(data_nm, 'start', None, None)

                # 전체인 경우는 먼저 삭제하고 업로드한다. // 또한 법정동코드도 무조건 삭제하고 다시 올린다.
                if batch_opt == 'full' or (batch_opt == 'daily' and data_nm == constant.LSCT_LAWDCD):
                    truncate_table(data_nm)

                # 시도별 데이터 처리
                for u_file in lst_files:
                    file_date = re.sub(r'[^0-9]', '', u_file[int_pos:])
                    # 진행조건 체크..
                    if not is_processing(batch_opt, file_date, std_month, std_date, data_nm):
                        continue

                    if batch_date is None or (batch_date is not None and batch_date == file_date):
                        if layer_yn == 'Y' and u_file[-4:] == '.shp':
                            dict_inifo = meta_info.layer_info.get(data_nm)
                            # full name
                            shp_full_nm = os.path.join(unzip_folder, u_file)
                            shp_df = gpd.read_file(shp_full_nm, encoding='euckr')
                            if shp_df is None:
                                logger.error(f"Wrong Shape File: {str(shp_full_nm)}")
                                continue
                            # Insert Data
                            # insert_shp_values(dict_inifo['table'], dict_inifo, shp_df, shp_full_nm, db_con)

                            # 메모리 해제
                            del [[shp_df]]
                            gc.collect()
                            shp_df = gpd.GeoDataFrame()
                            lst_prcs_date.append(file_date)
                            dict_data[data_nm].append(u_file[:-4])
                        # LSCT_LAWDCD 법정동코드는  .csv 파일임
                        elif layer_yn == 'N' and (u_file[-4:] == '.txt' or u_file[-4:] == '.csv'):
                            # 일자별 업데이트 파일은 _C 테이블에 추가한다.
                            sqlldr_nm = get_sqlldr_nm(file_date, data_nm)

                            ctl_path = f"{constant.sqlldr_ctr_folder}{sqlldr_nm}.ctl"
                            log_path = f"{constant.sqlldr_log_folder}/{sqlldr_nm}_{execute_day}.log"
                            bad_path = f"{constant.sqlldr_bad_folder}/{sqlldr_nm}_{execute_day}.bad"
                            data_path = f"{unzip_folder}/{u_file}"
                            command = rf"sqlldr userid={config.db_user}/{config.db_pass}@{config.db_dsn} control={ctl_path} data={data_path} log={log_path} bad={bad_path} rows=100000 direct=TRUE"
                            # Insert Data
                            os.system(command)

                            logger.info(f"{unzip_folder}/{u_file}: Sqlldr Success!!")
                            lst_prcs_date.append(file_date)
                            dict_data[data_nm].append(u_file[:-4])

                # 로그
                if len(lst_prcs_date) > 0:
                    db_log(data_nm, batch_opt, max(lst_prcs_date), None)
                else:
                    db_log(data_nm, batch_opt, None, None)
                del lst_prcs_date[:]

            except Exception as e:
                logger.error(f"error occured!!: {str(e)}")
                db_log(data_nm, 'error', None, str(e))

    # 작업완료된 파일 백업폴더로 이동
    # TODO: 운영시에 주석 해제
    # utils.move_and_backup_files(dict_data)

    logger.info('finish batch job!!')
