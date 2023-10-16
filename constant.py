# ---------------------------------------------------------------------------------------------------------------------------------------------------
# NS센터 데이터 업로드 배치프로그램
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# 국토부 관련 폴더
ABPM_LAND_FRST_LEDG = 'ABPM_LAND_FRST_LEDG'  # 토지(임야)대장
APMM_NV_JIGA_MNG = 'APMM_NV_JIGA_MNG'  # 개별공시지가
ABPH_LAND_MOV_HIST = 'ABPH_LAND_MOV_HIST'  # 토지이동연혁
ABPD_UNQ_NO_CHG_HIST = 'ABPD_UNQ_NO_CHG_HIST'  # 고유번호변동연혁
LARD_ADM_SECT_SGG = 'LARD_ADM_SECT_SGG'  # 시군구경계도
LSMD_ADM_SECT_UMD = 'LSMD_ADM_SECT_UMD'  # 읍면동경계도
LSMD_ADM_SECT_RI = 'LSMD_ADM_SECT_RI'  # 리경계도
LSCT_LAWDCD = 'LSCT_LAWDCD'  # 법정동코드
LSMD_CONT_UI101 = 'LSMD_CONT_UI101'  # 도로/용도구역
LSMD_CONT_UI201 = 'LSMD_CONT_UI201'  # 고속국도/접도구역
LSMD_CONT_LDREG = 'LSMD_CONT_LDREG'  # 연속지적도
ABPM_SHR_YMB = 'ABPM_SHR_YMB'  # 공유지연명부
ABPH_OWNER_HIST = 'ABPH_OWNER_HIST'  # 소유자연혁
ABPD_LAND_MOV_RELJIBN = 'ABPD_LAND_MOV_RELJIBN'  # 토지이동관련지번

# 전체 데이터 폴더 정리 (14개 데이터)
data_folder_list = {
    ABPM_LAND_FRST_LEDG,  # 토지(임야)대장
    APMM_NV_JIGA_MNG,  # 개별공시지가
    ABPH_LAND_MOV_HIST,  # 토지이동연혁
    ABPD_UNQ_NO_CHG_HIST,  # 고유번호변동연혁
    LARD_ADM_SECT_SGG,  # 시군구경계도
    LSMD_ADM_SECT_UMD,  # 읍면동경계도
    LSMD_ADM_SECT_RI,  # 리경계도
    LSCT_LAWDCD,  # 법정동코드
    LSMD_CONT_UI101,  # 도로/용도구역
    LSMD_CONT_UI201,  # 고속국도/접도구역
    LSMD_CONT_LDREG,  # 연속지적도
    ABPM_SHR_YMB,  # 공유지연명부
    ABPH_OWNER_HIST,  # 소유자연혁
    ABPD_LAND_MOV_RELJIBN  # 토지이동관련지번
}

# Prod ---------------------------------------------------------------------------------------------------------------------------------------------------------------

# # TARGET
# source_folder_path = r'/DATA/landinfo/temp'
#
# # DATA
# target_folder_path = r'/GIS_MAIN'
#
# # SQLLDR 관련 폴더
# sqlldr_ctr_folder = r'/DATA/landinfo/dailybatch/sqlldr/'
# sqlldr_log_folder = sqlldr_ctr_folder + r'logs'
# sqlldr_bad_folder = sqlldr_ctr_folder + r'bad'
#
# # SHP 생성 PATH
# create_shp_path = r'/DATA/landinfo/dailybatch/nsdi-data/shp/'
#
# # LOG 폴더 PATH
# log_path = r'/DATA/landinfo/dailybatch/log/'

# Centerline Dissolve Data Path
# center_line_shp = r'/DATA/landinfo/dailybatch/centerline/center_line_bf_dissolve.shp'

# Backup Folder
# backup_path = r'/DATA/landinfo/dailybatch/backup/'


# Dev ---------------------------------------------------------------------------------------------------------------------------------------------------------------

# TARGET (전체 데이터가 폴더별로 있다)
source_folder_path = r'C:/DATA/landinfo/temp/'

# DATA(날짜별 데이터별 unzip)
# (f'{FOLDER_PATH.DATA_FOLDER_PATH}/{target_date}/{FOLDER_PATH.LSMD_CONT_LDREG}/unzip/*.shp'
target_folder_path = r'C:/GIS_MAIN/'

# SQLLDR 관련 폴더
sqlldr_ctr_folder = r'C:/DATA/landinfo/dailybatch/sqlldr/'
sqlldr_log_folder = sqlldr_ctr_folder + r'logs'
sqlldr_bad_folder = sqlldr_ctr_folder + r'bad'

# SHP 생성 PATH
create_shp_path = r'C:/DATA/landinfo/dailybatch/nsdi-data/shp/'

# LOG 폴더 PATH
log_path = r'C:/DATA/landinfo/dailybatch/log/'

# Centerline Dissolve Data Path
center_line_shp = r'C:/DATA/landinfo/dailybatch/centerline/center_line_bf_dissolve.shp'

# Backup Folder
backup_path = r'C:/DATA/landinfo/backup/'
