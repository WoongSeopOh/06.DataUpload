# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램
# ---------------------------------------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import geopandas as gpd
import glob
import gc
import os
import cx_Oracle
import timeit
from numba import jit

import constant
import meta_info
import log
import utils

SR_ID = 2097
DB_GEOMETRY = 'SDO_GEOMETRY'
GEOM_TYPE_POINT = 'MULTIPOINT'
GEOM_TYPE_POLYLINE = 'MULTILINE'
GEOM_TYPE_POLYGON = 'MULTIPOLYGON'

# DB Connect
db_con = cx_Oracle.connect(user='land', password='land', dsn='land')
db_cur = db_con.cursor()

typeObj = db_con.gettype("MDSYS.SDO_GEOMETRY")
elementInfoTypeObj = db_con.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
ordinateTypeObj = db_con.gettype("MDSYS.SDO_ORDINATE_ARRAY")

logger = log.logger


# create_geom_obj-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def create_geom_obj(geom_type, geom):
    lst_pts = []
    lst_elem_info = []
    type_var = None
    # Geometry 객체 생성
    # typeObj = db_con.gettype("MDSYS.SDO_GEOMETRY")
    # elementInfoTypeObj = db_con.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
    # ordinateTypeObj = db_con.gettype("MDSYS.SDO_ORDINATE_ARRAY")

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


# create_geom_obj-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# def create_geom_obj3(geom_type, geom):
#     lst_pts = []
#     lst_elem_info = []
#     type_var = None
#
#     obj = typeObj.newobject()
#
#     # Geometry Type 정의
#     if geom_type == 'Polygon':
#         obj.SDO_GTYPE = 2003
#         type_var = 1003
#         lst_elem_info.extend([1, 1003, 1])
#     elif geom_type == 'MultiPolygon':
#         obj.SDO_GTYPE = 2007
#         type_var = 1003
#         lst_elem_info.extend([1, 1003, 1])
#
#     # SR_ID 전역변수
#     obj.SDO_SRID = SR_ID
#
#     obj.SDO_ELEM_INFO = elementInfoTypeObj.newobject()
#     obj.SDO_ORDINATES = ordinateTypeObj.newobject()
#
#     if geom_type in ['LineString', 'Polygon']:
#         for pt in geom.exterior.coords:
#             lst_pts.extend([pt[0], pt[1]])
#
#     if geom_type in ['MultiLineString', 'MultiPolygon']:
#         int_parts = len(geom.geoms)
#         i = 0
#         for g in geom.geoms:
#             for pt in g.exterior.coords:
#                 lst_pts.extend([pt[0], pt[1]])
#             if i < int_parts - 1:
#                 lst_elem_info.extend([len(lst_pts)+1, type_var, 1])
#             i += 1
#
#     obj.SDO_ELEM_INFO.extend(lst_elem_info)
#     obj.SDO_ORDINATES.extend(lst_pts)
#
#     return obj


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def insert_shp_values(layer_nm, layer_info, lyr_df, lyr_full_nm):
    value_list = []
    total_values = []

    # 필드에 해당하는 인서트 구문 생성
    dict_fields = layer_info.get('fields')
    fld_body_name, fld_values = '', ''
    inx = 0
    idx = 1
    for fld in dict_fields.keys():
        inx += 1
        fld_body_name = fld_body_name + fld + ','
        fld_values = fld_values + ':' + str(inx) + ','

    # 필드 수 만큼 바인드 변수 생성
    isrt_sql = "INSERT INTO " + meta_info.layer_nm.get(layer_nm) + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"
    # logger.info(isrt_sql)
    logger.info(lyr_full_nm + ' Object Creation Started!!')
    print('-------  A --------')
    print(str(len(lyr_df)))
    print('-------  B --------')
    print(str(lyr_df.shape))

    int_test = 1
    for index, row in lyr_df.iterrows():
        # for debug
        if divmod(int_test, 10000)[1] == 0:
            break
        logger.info(lyr_full_nm + str(int_test))

        for fld in dict_fields.keys():
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
        # if idx == 2:
        #     logger.info(value_list)
        copy_list = value_list[:]
        total_values.append(copy_list)
        del value_list[:]
        int_test += 1

    logger.info(lyr_full_nm + ' Geometry Object Creation Finished!! - ' + str(len(total_values)) + ' 건')
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
            db_cur.executemany(isrt_sql, split_rows)
            db_con.commit()
            total_size = total_size + len(split_rows)
            logger.info(lyr_full_nm + ' Uploading... : ' + str(total_size) + ' 건')
        logger.info(lyr_full_nm + ' Upload Completed!!: ' + str(total_size) + ' 건')

    except cx_Oracle.DatabaseError as e:
        logger.error(isrt_sql)
        logger.error('error occured!!' + str(e))
        exit()
    finally:
        del value_list[:]


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def insert_shp_values2(layer_nm, layer_info, lyr_df, lyr_full_nm):
    value_list = []
    total_values = []

    # 필드에 해당하는 인서트 구문 생성
    dict_fields = layer_info.get('fields')
    fld_body_name, fld_values = '', ''
    inx = 0
    idx = 1
    for fld in dict_fields.keys():
        inx += 1
        fld_body_name = fld_body_name + fld + ','
        fld_values = fld_values + ':' + str(inx) + ','

    # 필드 수 만큼 바인드 변수 생성
    # isrt_sql = "INSERT INTO " + meta_info.layer_nm.get(layer_nm) + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"
    # logger.info(isrt_sql)
    isrt_sql = 'INSERT INTO T_LNDB_L_LARD_ADM_SECT_SGG(ADM_SECT_CD, SGG_NM, SGG_OID, COL_ADM_SECT_CD, OBJECTID, SHAPE) VALUES(: 1,:2,: 3,:4,: 5, SDO_GEOMETRY(:6, 2097))'

    logger.info(lyr_full_nm + ' Object Creation Started!!')
    print('-------  A --------')
    print(str(len(lyr_df)))
    print('-------  B --------')
    print(str(lyr_df.shape))

    int_test = 0
    for index, row in lyr_df.iterrows():
        # for debug
        if divmod(int_test, 1000)[1] == 0:
            logger.info(lyr_full_nm + str(int_test))

        for fld in dict_fields.keys():
            if fld == 'SHAPE':
                # g_type = row.geometry.geom_type
                # g_geom = row.geometry
                # geom = create_geom_obj(g_type, g_geom)
                value_list.append(row['geometry'].wkt)
            else:
                if dict_fields.get(fld) in row.keys():
                    if utils.isNaN(row.get(dict_fields.get(fld))):
                        value_list.append(None)
                    else:
                        value_list.append(row.get(dict_fields.get(fld)))
                else:
                    value_list.append(idx)
                    idx += 1
        # if idx == 2:
        #     logger.info(value_list)
        copy_list = value_list[:]
        total_values.append(copy_list)
        del value_list[:]
        int_test += 1

    logger.info(lyr_full_nm + ' Geometry Object Creation Finished!! - ' + str(len(total_values)) + ' 건')
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
            db_cur.executemany(isrt_sql, split_rows)
            db_con.commit()
            total_size = total_size + len(split_rows)
            logger.info(lyr_full_nm + ' Uploading... : ' + str(total_size) + ' 건')
        logger.info(lyr_full_nm + ' Upload Completed!!: ' + str(total_size) + ' 건')

    except cx_Oracle.DatabaseError as e:
        logger.error(isrt_sql)
        logger.error('error occured!!' + str(e))
        exit()
    finally:
        del value_list[:]


# def get_insert_value(arg_dict, arg_row, arg_idx):
#     rtn_list = list()
#
#     for fld in arg_dict.keys():
#         if fld == 'SHAPE':
#             g_type = arg_row.geometry.geom_type
#             g_geom = arg_row.geometry
#             geom = create_geom_obj(g_type, g_geom)
#             rtn_list.append(geom)
#         else:
#             if arg_dict.get(fld) in arg_row.keys():
#                 if utils.isNaN(arg_row.get(arg_dict.get(fld))):
#                     rtn_list.append(None)
#                 else:
#                     rtn_list.append(arg_row.get(arg_dict.get(fld)))
#             else:
#                 arg_idx += 1
#                 rtn_list.append(arg_idx)
#
#     return rtn_list


def insert_shp_values3(layer_nm, layer_info, lyr_df, lyr_full_nm):
    value_list = list()
    total_values = list()
    upload_values = list()
    batch_size = 2000

    # 필드에 해당하는 인서트 구문 생성
    dict_fields = layer_info.get('fields')
    fld_body_name, fld_values = '', ''
    inx = 0
    idx = 1
    for fld in dict_fields.keys():
        inx += 1
        fld_body_name = fld_body_name + fld + ','
        fld_values = fld_values + ':' + str(inx) + ','

    # 필드 수 만큼 바인드 변수 생성
    isrt_sql = "INSERT INTO " + meta_info.layer_nm.get(layer_nm) + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"
    logger.info(isrt_sql)
    icnt = 0
    total_count = 0
    data_count = len(lyr_df)
    share, rest = divmod(data_count, batch_size)
    logger.info(lyr_full_nm + ' Upload Started!!: ' + str(total_count) + ' 건')

    for index, row in lyr_df.iterrows():
        for fld in dict_fields.keys():
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

        total_values.extend([value_list[:]])
        del value_list[:]

        # total_values.extend(get_insert_value(dict_fields, row, index))

        icnt += 1
        if divmod(icnt, 2000)[1] == 0:
            # print('insert??')
            # db_cur.executemany(isrt_sql, total_values)
            # db_con.commit()
            # print('insert!!')

            total_count += len(total_values)
            print(str(total_count))
            # # upload_values.extend(total_values[:])
            del total_values[:]
            gc.collect()
            # print(str(len(upload_values)))

        # st1 = timeit.default_timer()
        # st2 = timeit.default_timer()
        # print("RUN TIME : {0}".format(st2 - st1))

        # # 배치사이즈별로 DB에 업로드.. 나머지가 0이거나, 몫과 나머지가 동일한 경우
        # if divmod(icnt, batch_size)[1] == 0 or (divmod(icnt, batch_size)[0] == share and divmod(icnt, batch_size)[1] == rest):
        #     try:
        #         logger.info('A')
        #         db_cur.executemany(isrt_sql, total_values)
        #         db_con.commit()
        #         total_count += len(total_values)
        #     except cx_Oracle.DatabaseError as e:
        #         logger.error(isrt_sql)
        #         logger.error('error occured!!' + str(e))
        #         exit()
        #     # 메모리 초기화
        #     del total_values[:]
        #     gc.collect()
        #     total_values = []
        #
        #     logger.info('Uploading... : ' + str(total_count) + ' 건')

    # logger.info(lyr_full_nm + ' Upload Completed!!: ' + str(total_count) + ' 건')


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
logger.info('start batch job!!')
str_today = str(datetime.now().strftime("%Y%m%d"))

# 작업폴더 생성
utils.create_job_folder(constant.target_folder_path + str_today)

# 데이터별로 처리
try:
    for data_nm in constant.spatial_data_list:
        logger.info(data_nm + ': Database Processing Startd!! --------------------------------------------------------------------------------------------------------')
        # target_nm = /DATA/landinfo/temp/+ data/*.*  (temp 아래 데이터명 폴더가 하나 더 있고, 그 아래 zip 파일이 전송됨)
        source_folder_nm = constant.source_folder_path + data_nm
        # folder_nm = /GIS_MAIN + '/' + str_today + '/' + data/*.* (날짜 아래 데이터명 폴더가 하나 더 있고, 그 아래 파일 복사, unzip 폴더 생성하고, 해당 폴더에 압축 해제)
        target_folder_nm = constant.target_folder_path + str_today + '/' + data_nm
        utils.create_job_folder(target_folder_nm)
        utils.create_job_folder(target_folder_nm + '/unzip')

        unzip_folder = utils.get_folder_move_and_unzip(source_folder_nm, target_folder_nm, data_nm)

        logger.info(unzip_folder + ': Unzip and data moving finished!!')

        if unzip_folder is not None:
            lst_files = os.listdir(unzip_folder)
            for u_file in lst_files:
                if u_file[-4:] == '.shp':
                    dict_inifo = meta_info.layer_info.get(data_nm)
                    if dict_inifo['layer']:
                        # full name
                        shp_full_nm = os.path.join(unzip_folder, u_file)
                        shp_df = gpd.read_file(shp_full_nm, encoding='euckr')
                        logger.info(shp_full_nm + ': Geopandas data read')
                        # shp_source = ogr.Open(shp_full_nm)
                        if shp_df is None:
                            logger.error("Wrong Shape File :" + str(shp_full_nm))
                            continue
                        # insert_shp_values(data_nm, dict_inifo, shp_df, shp_full_nm)
                        # insert_shp_values2(data_nm, dict_inifo, shp_df, shp_full_nm)
                        # jit_rand_events = jit(nopython=True)(insert_shp_values3)
                        insert_shp_values3(data_nm, dict_inifo, shp_df, shp_full_nm)

                        # 메모리 해제
                        del [[shp_df]]
                        gc.collect()
                        shp_df = gpd.GeoDataFrame()
                else:
                    # sqlldr
                    pass

except Exception as e:
    logger.error('error occured!!' + str(e))
    exit()

logger.info('finish batch job!!')
