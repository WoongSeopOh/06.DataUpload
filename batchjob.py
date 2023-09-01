# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램
# ---------------------------------------------------------------------------------------------------------------------------------------------------
import logging
import os
import shutil
import zipfile
import time
import cx_Oracle
from datetime import datetime

import constant

SR_ID = 5179
DB_GEOMETRY = 'SDO_GEOMETRY'
GEOM_TYPE_POINT = 'MULTIPOINT'
GEOM_TYPE_POLYLINE = 'MULTILINE'
GEOM_TYPE_POLYGON = 'MULTIPOLYGON'

# logger 셋팅
str_today = str(datetime.now().strftime("%Y%m%d"))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s: %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

log_folder = constant.log_path
log_nm = str_today + "_batchjob.log"
file_handler = logging.FileHandler(log_folder + log_nm)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# DB Connect
db_con = cx_Oracle.connect(user='land', password='land', dsn='oratest')
db_cur = db_con.cursor()


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def create_job_folder(fldr_nm):
    # folder_nm = constant.data_folder_path + '/' + str_today + '/' + data
    tmp_char = str(datetime.now().strftime("%Y%m%d%H%M%S"))
    try:
        # 1) 작업 폴더 생성
        if not os.path.exists(fldr_nm):
            os.mkdir(fldr_nm)
        else:
            new_name_for_delete = tmp_char
            os.rename(fldr_nm, new_name_for_delete)
            time.sleep(3)
            os.mkdir(fldr_nm)
            shutil.rmtree(new_name_for_delete, ignore_errors=True)

        return

    except shutil.Error as exc:
        logger.error("Folder Creation Error Occurred! :" + str(exc.args[0]))


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def get_folder_move_and_unzip(src_fldr, tgt_fldr, arg_data):

    if os.path.isdir(src_fldr):
        all_files = [src_file for src_file in os.listdir(src_fldr) if src_file.startswith(arg_data)]

        # 데이터별로 최종 파일 체크 (필요시)
        move_files = get_movefile_nm(all_files, arg_data)

        for m_file in move_files:
            shutil.copy(src_fldr + "/" + m_file, tgt_fldr)
        unzip_fldr = tgt_fldr + "/unzip"

        # 데이터 압축해제
        data_files = os.listdir(tgt_fldr)
        for zip_file in data_files:
            zip_file_full_nm = tgt_fldr + "/" + zip_file
            try:
                if zip_file_full_nm.endswith('.zip'):
                    zip_ref = zipfile.ZipFile(zip_file_full_nm)
                    zip_ref.extractall(unzip_fldr)
                    zip_ref.close()
            except zipfile.BadZipfile:
                logger.error("Folder Creation Error Occurred! :" + str(zip_file_full_nm))
        return unzip_fldr

    return None


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def get_movefile_nm(arg_all_files, arg_data_nm):
    # rtn_list = []
    # tgt_files = []
    # 데이터별로 최종 데이터를 선별한다... 데이터 넘어오는 것 보고 필요시 활용
    if arg_data_nm == 'LSMD_CONT_LDREG':
        pass
        # 월별 데이터만 해당됨(자리수 29)
        # for m_file in arg_all_files:
        #     if len(m_file) == 29:
        #         # argMoveFiles.remove(mFile)
        #         tgt_files.append(m_file)
        #
        # # 동일한 이름의 데이터가 있는 경우 큰 값을 가져온다.  // sidoList 활용
        # for sido_code in ['11']:
        #     file_list = [nfile for nfile in tgt_files if nfile.startswith(arg_data_nm + '_' + sido_code)]
        #     if len(file_list) > 0:
        #         file_list.sort(reverse=True)
        #         rtn_list.append(file_list[0])
    else:
        pass
        # rtn_list = arg_all_files

    return arg_all_files


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
            # Multi인 경우 q
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


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
logger.info('start batch job!!')

# 작업폴더 생성
create_job_folder(constant.target_folder_path + str_today)

for data in constant.data_folder_list:
    # target_nm = /DATA/landinfo/temp/+ data/*.*  (temp 아래 데이터명 폴더가 하나 더 있고, 그 아래 zip 파일이 전송됨)
    source_folder_nm = constant.source_folder_path + data
    # folder_nm = /GIS_MAIN + '/' + str_today + '/' + data/*.* (날짜 아래 데이터명 폴더가 하나 더 있고, 그 아래 파일 복사, unzip 폴더 생성하고, 해당 폴더에 압축 해제)
    target_folder_nm = constant.target_folder_path + str_today + '/' + data
    create_job_folder(target_folder_nm)
    create_job_folder(target_folder_nm + '/unzip')

    unzip_folder = get_folder_move_and_unzip(source_folder_nm, target_folder_nm, data)

    if unzip_folder is not None:
        for u_file in os.listdir(unzip_folder):
            pass

logger.info('finish batch job!!')
