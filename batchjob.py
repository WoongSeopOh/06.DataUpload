# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램
# ---------------------------------------------------------------------------------------------------------------------------------------------------
from osgeo import ogr
from datetime import datetime
import logging
import os
import shutil
import zipfile
import time
import cx_Oracle

import constant
import table_meta_info

SR_ID = 5179
DB_GEOMETRY = 'SDO_GEOMETRY'
GEOM_TYPE_POINT = 'MULTIPOINT'
GEOM_TYPE_POLYLINE = 'MULTILINE'
GEOM_TYPE_POLYGON = 'MULTIPOLYGON'



# DB Connect
db_con = cx_Oracle.connect(user='ex2023', password='ex2023', dsn='land')
db_cur = db_con.cursor()


# insert_values--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def insert_shp_values(layer_nm, layer_info, layer_data):
    # 필드목록 생성
    value_list = []
    total_values = []

    # 필드에 해당하는 인서트 구문 생성
    fld_body_name, fld_values = '', ''
    inx = 0
    for fld in layer_info.get('fields'):
        inx += 1
        fld_body_name = fld_body_name + fld + ','
        fld_values = fld_values + ':' + str(inx) + ','

    # 필드 수 만큼 바인드 변수 생성
    isrt_sql = "INSERT INTO " + layer_nm + "(" + fld_body_name[:-1] + ") VALUES(" + fld_values[:-1] + ")"

    for feature in layer_data:
        # 필드정의는 Shape 필드 순서대로 확인 후 정의
        for i in range(len(layer_info.get('fields'))):
            value_list.append(feature.GetField(i))
        # 마지막 요소로 Geometry 객체 추가
        geom_obj = create_geom_obj(layer_info.get('type'), feature.GetGeometryRef())
        value_list.append(geom_obj)
        # 전체 리스트 추가 (list Copy)
        copy_list = value_list[:]
        total_values.append(copy_list)

    print(total_values)
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
    if arg_type == 'point':
        obj.SDO_GTYPE = 2001
    elif arg_type == 'polyline':
        obj.SDO_GTYPE = 2006
        lst_elem_info.extend([1, 2, 1])
    elif arg_type == 'polygon':
        obj.SDO_GTYPE = 2007
        lst_elem_info.extend([1, 1003, 1])

    # SR_ID 전역변수
    obj.SDO_SRID = SR_ID
    try:
        # 데이터 타입별로 처리
        # TODO: MultiPoint
        if arg_type == 'point':
            obj.SDO_POINT = pointTypeObj.newobject()
            obj.SDO_POINT.X = arg_geometry.Centroid().GetX()
            obj.SDO_POINT.Y = arg_geometry.Centroid().GetY()
        elif arg_type == 'polyline' or arg_type == 'polygon':
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
        return None


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
logger.info('start batch job!!')

# 작업폴더 생성
create_job_folder(constant.target_folder_path + str_today)

# 데이터별로 처리
for data_nm in constant.spatial_data_list:
    # target_nm = /DATA/landinfo/temp/+ data/*.*  (temp 아래 데이터명 폴더가 하나 더 있고, 그 아래 zip 파일이 전송됨)
    source_folder_nm = constant.source_folder_path + data_nm
    # folder_nm = /GIS_MAIN + '/' + str_today + '/' + data/*.* (날짜 아래 데이터명 폴더가 하나 더 있고, 그 아래 파일 복사, unzip 폴더 생성하고, 해당 폴더에 압축 해제)
    target_folder_nm = constant.target_folder_path + str_today + '/' + data_nm
    create_job_folder(target_folder_nm)
    create_job_folder(target_folder_nm + '/unzip')

    unzip_folder = get_folder_move_and_unzip(source_folder_nm, target_folder_nm, data_nm)

    if unzip_folder is not None:
        lst_files = os.listdir(unzip_folder)
        for u_file in lst_files:
            if u_file[-4:] == '.shp':
                dict_inifo = table_meta_info.meta_info.get(data_nm)
                if dict_inifo['layer']:
                    # full name
                    shp_full_nm = os.path.join(unzip_folder, u_file)
                    shp_source = ogr.Open(shp_full_nm)
                    if shp_source is None:
                        logger.error("Wrong Shape File :" + str(shp_full_nm))
                        continue

                    shp_layer = shp_source.GetLayer()
                    # 좌표계
                    # spatialRef = shp_layer.GetSpatialRef()
                    # print(spatialRef)
                    insert_shp_values(shp_full_nm, dict_inifo, shp_layer)
            else:
                # sqlldr
                pass

logger.info('finish batch job!!')
