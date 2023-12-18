# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 국토부 제출 shape 파일 생성
# ---------------------------------------------------------------------------------------------------------------------------------------------------
import sys
import geopandas as gpd
from shapely import wkt
import cx_Oracle

import constant
import log
import config


# Entry Point ---------------------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    logger = log.logger2
    # 토지 관리(연결용지도) SHP 생성
    logger.info("NS센터 제공 데이터 추출 처리 시작")

    # DB Connect
    db_con = cx_Oracle.connect(user=config.db_user, password=config.db_pass, dsn=config.db_dsn)
    db_cursor = db_con.cursor()
    db_cur = db_con.cursor()

    try:
        db_cursor.execute("SELECT LTLND_UNQ_NO, ADMN_AREA_ADDR, LDNO_NM, CLLD_NM, LDAR, OWNS_NM, JNON_TNOP, INDV_OALP, INDV_OALP_PBNT_DATES, ROAD_AREA_YN, JRSD_DPTNM, JRSD_MTNOF_NM, ROTNM, USAG_CTNT, USAG_DETL_CTNT, ADIN_REGS_YN, LND_UTL_STAT_NM, MGMT_ACQS_STAT_NM, MTCH_YN, SDO_UTIL.TO_WKTGEOMETRY( SPIN_GMTR_VAL ) AS SPIN_GMTR_VAL, TO_CHAR( CRET_DTTM, 'YYYYMMDD' ) AS CRET_DTTM FROM GIS_MAIN.T_LNDB_LMGM01M WHERE SPIN_GMTR_VAL IS NOT NULL")
        db_cur.execute("SELECT OBJECTID, SDO_UTIL.TO_WKTGEOMETRY(SPIN_GMTR_VAL) AS SPIN_GMTR_VAL, TO_CHAR( CRET_DTTM, 'YYYYMMDD' ) AS CRET_DTTM  FROM GIS_MAIN.T_LNDB_LMGMB01M")

    except cx_Oracle.DatabaseError as e:
        logger.error(f"Database Error Occured: {str(e)}")
        db_cursor.close()
        db_cur.close()
        db_con.close()
        sys.exit()

    try:
        # 용지도 추출시작
        land_mng = gpd.GeoDataFrame(db_cursor.fetchall(), columns=['LTLND_UNQ_', 'ADMN_AREA_', 'LDNO_NM', 'CLLD_NM', 'LDAR', 'OWNS_NM', 'JNON_TNOP', 'INDV_OALP', 'INDV_OALP_', 'ROAD_AREA_', 'JRSD_DPTNM', 'JRSD_MTNOF', 'ROTNM', 'USAG_CTNT', 'USAG_DETL_', 'ADIN_REGS_', 'LND_UTL_ST', 'MGMT_ACQS_', 'MTCH_YN', 'SPIN_GMTR_VAL', 'CRET_DTTM'])
        land_mng['geometry'] = gpd.GeoSeries(land_mng['SPIN_GMTR_VAL'].apply(lambda x: wkt.load(x)))
        land_mng.set_crs(epsg='EPSG:5179', allow_override=True)
        land_mng.crs = 'EPSG:5179'

        del land_mng['SPIN_GMTR_VAL']
        land_mng.to_file(r'{0}land_mng.shp'.format(constant.create_shp_path), driver='ESRI Shapefile', encoding='utf-8')
        db_cursor.close()
        logger.info("토지 관리(연결용지도) SHP 생성 완료")

        # 용지경계선 추출 시작
        mng_line = gpd.GeoDataFrame(db_cur.fetchall(), columns=['OBJECTID', 'SPIN_GMTR_VAL', 'CRET_DTTM'])
        mng_line['geometry'] = gpd.GeoSeries(mng_line['SPIN_GMTR_VAL'].apply(lambda x: wkt.load(x)))
        mng_line.set_crs(epsg='EPSG:5179', allow_override=True)
        mng_line.crs = 'EPSG:5179'
        del mng_line['SPIN_GMTR_VAL']

        mng_line.to_file(r'{0}land_mng_line.shp'.format(constant.create_shp_path), driver='ESRI Shapefile', encoding='utf-8')
        db_cur.close()
        logger.info("토지 관리(연결용지경계선) SHP 생성 완료")

        logger.info("NS센터 제공 데이터 추출 처리 완료")

        db_con.close()

    except Exception as e:
        logger.error(f"Error occured!!: {str(e)}")
        db_cursor.close()
        db_cur.close()
        db_con.close()

    sys.exit()
