# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램 (월배치)
# 대상 데이터: APMM_NV_JIGA_MNG(속성), LARD_ADM_SECT_SGG, LSMD_ADM_SECT_UMD, LSMD_ADM_SECT_RI, LSCT_LAWDCD, LSMD_CONT_UI101, LSMD_CONT_UI201, LSMD_CONT_LDREG
# 설명: 월변동 데이터를 업로드하고, 용지도, 용지경계 등을 새로 생성한다.
# 파라미터: (YYYYMM) -  숫자 6자리가 아닌 경우 Return
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

SR_ID = 5174

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
execute_day = str(datetime.now().strftime("%Y%m%d%H%M%S"))

os.environ['NLS_LANG'] = "AMERICAN_AMERICA.KO16MSWIN949"


# get_sqlldr_nm ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_sqlldr_nm(arg_date, arg_nm):
    if arg_nm == constant.LSCT_LAWDCD:
        return arg_nm
    else:
        if len(arg_date) == 8:
            if arg_nm == constant.ABPD_UNQ_NO_CHG_HIST:
                return arg_nm + 'C'
            else:
                return arg_nm + '_C'
        else:
            return arg_nm


# truncate_table ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def truncate_table(arg_data_nm):
    db_cursor = db_con.cursor()

    if arg_data_nm == constant.APMM_NV_JIGA_MNG:
        return None

    try:
        truncate_sql = "TRUNCATE TABLE GIS_MAIN." + meta_info.real_data_nm.get(arg_data_nm)
        db_cursor.execute(truncate_sql)
        db_cursor.close()

    except cx_Oracle.DatabaseError as Err:
        logger.error(f"Error occured!!: {str(Err)}")
        db_cursor.close()
        db_con.close()
        sys.exit()


# is_processing ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def is_processing(opt, date_value, arg_month, arg_data_nm):
    daily_len = 8
    full_len = 6

    # 예외케이스) 정의
    if arg_data_nm == constant.LSCT_LAWDCD:
        return True
    if arg_data_nm == constant.APMM_NV_JIGA_MNG:
        daily_len = 6

    if date_value is not None and len(date_value) == full_len:
        if date_value > arg_month:
            return True
        else:
            logger.info(f"{unzip_folder}/{u_file}: T_LNDB_L_BATCH_MNG 테이블의 최종 업데이트 월보다 이전 정보는 업데이트할 수 없습니다.")

    return False


# get_parameters ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_parameters(argvs):

    if len(argvs) > 1:
        arg_date = argvs[1]
        if arg_date.isdigit() and len(arg_date) == 6:
            return arg_date
        else:
            logger.error(f"파라미터는 YYYYMM 구성의 여섯자리 숫자여야 합니다.")
            sys.exit()
    else:
        logger.error(f"파라미터는 YYYYMM 구성의 여섯자리 숫자여야 합니다.")
        sys.exit()


# get_batch_master ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_batch_master(arg_param):
    db_cursor = db_con.cursor()

    rtn_list = list()
    try:
        strSQL = "SELECT DATA_NM, LAYER_YN, LAST_DAILY_UPDATE, LAST_FULL_UPDATE FROM T_LNDB_L_BATCH_MNG WHERE USE_YN = 'Y' AND ALL_BATCH_YN = 'Y' AND LAST_FULL_UPDATE < " + arg_param

        db_cursor.execute(strSQL)
        for rec in db_cursor:
            rtn_list.append(rec)
        db_cursor.close()

    except cx_Oracle.DatabaseError as e:
        logger.error(f"Database Error Occured: {str(e)}")
        db_cursor.close()
        db_con.close()
        sys.exit()

    return rtn_list


# db_log----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def db_log(arg_data_nm, prcs_type, arg_file_date, err_msg=None):
    db_cursor = db_con.cursor()

    try:
        # prcs_type : 1) 시작, 2) 변동분, 3) 전체분, 4) 에러
        if prcs_type == 'start':
            u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET BGN_BATCH_DATE = SYSDATE, LAST_UPDATE_DATE = SYSDATE, END_BATCH_DATE = NULL, SCSS_YN = NULL, RMK = NULL WHERE DATA_NM = :1"
            db_cursor.execute(u_sql, [arg_data_nm])
        elif prcs_type == 'finish':
            if arg_file_date is not None:
                u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET LAST_FULL_UPDATE = :1, END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :2"
                db_cursor.execute(u_sql, [arg_file_date, arg_data_nm])
            else:
                u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'Y', LAST_UPDATE_DATE = SYSDATE WHERE DATA_NM = :1"
                db_cursor.execute(u_sql, [arg_data_nm])
        elif prcs_type == 'error':
            u_sql = "UPDATE T_LNDB_L_BATCH_MNG SET END_BATCH_DATE = SYSDATE, SCSS_YN = 'N', LAST_UPDATE_DATE = SYSDATE, RMK = :1 WHERE DATA_NM = :2"
            db_cursor.execute(u_sql, [err_msg, arg_data_nm])

        db_cursor.close()
        db_con.commit()

    except cx_Oracle.DatabaseError as Err:
        logger.error(f"Error occured!!: {str(Err)}")
        db_cursor.close()
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
                    lst_elem_info.extend([len(lst_pts) + 1, type_var, 1])
                i += 1

        obj.SDO_ELEM_INFO.extend(lst_elem_info)
        obj.SDO_ORDINATES.extend(lst_pts)
    else:
        return None

    return obj


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def insert_shp_values(arg_date, dic_info, lyr_df, lyr_full_nm, db):
    value_list = []
    total_values = []
    table_name = None
    dict_fields = None
    cursor = db.cursor()

    if len(arg_date) == 6:
        table_name = dic_info['table']
        dict_fields = dic_info.get('fields')
    elif len(arg_date) == 8:
        table_name = dic_info['c_table']
        dict_fields = dic_info.get('c_fields')

    # 필드에 해당하는 인서트 구문 생성
    fld_body_name, fld_values = '', ''
    inx = 0
    idx = 1
    for fld in dict_fields.keys():
        inx += 1
        fld_body_name = fld_body_name + fld + ','
        fld_values = fld_values + ':' + str(inx) + ','

    # 필드 수 만큼 바인드 변수 생성
    isrt_sql = "INSERT INTO " + table_name + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"
    int_test = 1
    for index, row in lyr_df.iterrows():
        for fld in dict_fields.keys():
            if fld == 'SHAPE':
                if row.geometry is not None:
                    g_type = row.geometry.geom_type
                    g_geom = row.geometry
                    geom = create_geom_obj(g_type, g_geom)
                else:
                    geom = None
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


def is_truncate(arg_files, arg_data_nm, arg_batch_date):
    pass
    # TODO: 공시자가의 경우 TRUNCATE 하지 않고, 계속 쌓는다.
    return False


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    dict_data = dict()
    logger.info('start batch_job_day!!')
    # 파라미터 셋팅 (변동분처리인지, 전체인지) / 특정날짜값이 있는지..
    batch_date = get_parameters(sys.argv)
    logger.info(f"parameters : {batch_date}")

    lst_prcs_date = list()
    # 전체 데이터 올리는 대상 중 batch_date 날짜보다 이전 데이터만 가져온다.
    lst_batch_mng = get_batch_master(batch_date)

    # 2) 작업폴더 생성
    utils.create_job_folder(constant.target_folder_path + execute_day)
    logger.info("create job folder")

    # 데이터별로 처리
    for data in lst_batch_mng:
        data_nm = data[0]
        layer_yn = data[1]
        # std_date = data[2] if data[2] is not None else '00000000'
        std_month = data[3] if data[3] is not None else '000000'

        # 실행파라미터에 데이터가 명시되었을 경우 해당 데이터만 처리함.
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
            if is_truncate(lst_files, data_nm, batch_date):
                truncate_table(data_nm)

            # 시도별 데이터 처리
            for u_file in lst_files:
                file_date = re.sub(r'[^0-9]', '', u_file[int_pos:])
                # 진행조건 체크..
                if not is_processing(batch_date, file_date, std_month, data_nm):
                    continue

                if batch_date == file_date:
                    # 레이어 (6개)
                    if layer_yn == 'Y' and u_file[-4:] == '.shp':
                        dict_inifo = meta_info.layer_info.get(data_nm)
                        # full name
                        shp_full_nm = os.path.join(unzip_folder, u_file)
                        shp_df = gpd.read_file(shp_full_nm, encoding='euckr')

                        # 좌표변환 (원본좌표 그대로 사용)
                        # shp_df = shp_df.set_crs(epsg=FROM_SR_ID, allow_override=True)
                        # shp_df.geometry = shp_df.geometry.to_crs(epsg=TO_SR_ID)

                        if shp_df is None:
                            logger.error(f"Wrong Shape File: {str(shp_full_nm)}")
                            continue

                        # Insert Data (daily와, full을 다르게 처리함)
                        insert_shp_values(file_date, dict_inifo, shp_df, shp_full_nm, db_con)

                        # 메모리 해제
                        del [[shp_df]]
                        gc.collect()
                        shp_df = gpd.GeoDataFrame()
                        lst_prcs_date.append(file_date)
                        dict_data[data_nm].append(u_file[:-4])

                    # 테이블(5개)
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
                db_log(data_nm, 'finish', max(lst_prcs_date), None)
            else:
                db_log(data_nm, 'finish', None, None)
            del lst_prcs_date[:]

        except Exception as e:
            logger.error(f"Error occured!!: {str(e)}")
            db_log(data_nm, 'error', None, str(e))

    # 작업완료된 파일 백업폴더로 이동
    logger.info("processing files backup")
    utils.move_and_backup_files(dict_data)

    # 프로시져 콜 필요
    logger.info('call batch job procedure')
    cursor = db_con.cursor()
    try:
        cursor.callproc('GIS_MAIN.P_UPDATE_MONTH')
        cursor.close()
    except cx_Oracle.DatabaseError as err:
        logger.error(f"Error occured!!: {str(err)}")
        cursor.close()
        db_con.close()

    logger.info('finish procedure')
    logger.info('finish batch job!!')
    exit()
