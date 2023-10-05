# ---------------------------------------------------------------------------------------------------------------------------------------------------
# 개별 테이블 정보
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# # {'ADM_SECT_C': '11110', 'SGG_NM': '\udcc1\udcbe\udcb7α\udcb8', 'SGG_OID': None, 'COL_ADM_SE': '11110'}
layer_info = {
    # 필드 순서에 맞게 정리(원본 Shape 필드 순서 확인 후 정리
    'LARD_ADM_SECT_SGG': {'layer': True,
                          'type': 'polygon',
                          'table': 'T_LNDB_L_LARD_ADM_SECT_SGG',
                          'fields': {'ADM_SECT_CD': 'ADM_SECT_C',
                                     'SGG_NM': 'SGG_NM',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                     'BATCH_DATE': None,
                                     'SHAPE': 'geometry'}},
    'LSMD_ADM_SECT_UMD': {'layer': True,
                          'type': 'polygon',
                          'table': 'T_LNDB_L_LSMD_ADM_SECT_UMD',
                          'fields': {'EMD_CD': 'EMD_CD',
                                     'EMD_NM': 'EMD_NM',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                     'BATCH_DATE': None,
                                     'SHAPE': 'geometry'}},
    'LSMD_ADM_SECT_RI': {'layer': True,
                         'type': 'polygon',
                         'table': 'T_LNDB_L_LSMD_ADM_SECT_RI',
                         'fields': {'RI_CD': 'RI_CD',
                                    'RI_NM': 'RI_NM',
                                    'SGG_OID': 'SGG_OID',
                                    'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                    'BATCH_DATE': None,
                                    'SHAPE': 'geometry'}},
    'LSMD_CONT_UI101': {'layer': True,
                        'type': 'polygon',
                        'table': 'T_LNDB_L_LSMD_CONT_UI101',
                        'fields': {'MNUM': 'MNUM',
                                   'ALIAS': 'ALIAS',
                                   'REMARK': 'REMARK',
                                   'NTFDATE': 'NTFDATE',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                   'BATCH_DATE': None,
                                   'SHAPE': 'geometry'}},
    'LSMD_CONT_UI201': {'layer': True,
                        'type': 'polygon',
                        'table': 'T_LNDB_L_LSMD_CONT_UI201',
                        'fields': {'MNUM': 'MNUM',
                                   'ALIAS': 'ALIAS',
                                   'REMARK': 'REMARK',
                                   'NTFDATE': 'NTFDATE',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                   'BATCH_DATE': None,
                                   'SHAPE': 'geometry'}},
    'LSMD_CONT_LDREG': {'layer': True,
                        'type': 'polygon',
                        'table': 'T_LNDB_L_LSMD_CONT_LDREG',
                        'fields': {'PNU': 'PNU',
                                   'JIBUN': 'JIBUN',
                                   'BCHK': 'BCHK',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                   'BATCH_DATE': None,
                                   'SHAPE': 'geometry'}}
}

# 파일 이름 안에 날짜를 가져오기위한 자리수 위치
file_date_pos = {
    # ABPM_LAND_FRST_LEDG_26_202308_Y.zip
    'ABPM_LAND_FRST_LEDG': 23,
    # APMM_NV_JIGA_MNG_11_2018_07.zip
    'APMM_NV_JIGA_MNG': 20,
    # ABPH_LAND_MOV_HIST_26_202308.zip
    'ABPH_LAND_MOV_HIST': 22,
    # ABPD_UNQ_NO_CHG_HIST_26_202308.zip
    'ABPD_UNQ_NO_CHG_HIST': 24,
    # LARD_ADM_SECT_SGG_27_202307.zip
    'LARD_ADM_SECT_SGG': 21,
    # LSMD_ADM_SECT_UMD_30_202308.zip
    'LSMD_ADM_SECT_UMD': 21,
    # LSMD_ADM_SECT_RI_41_202308.zip
    'LSMD_ADM_SECT_RI': 20,
    'LSCT_LAWDCD': 0,
    # LSMD_CONT_UI101_31_202308.zip
    'LSMD_CONT_UI101': 19,
    # LSMD_CONT_UI201_47_202308.zip
    'LSMD_CONT_UI201': 19,
    # LSMD_CONT_LDREG_30_202308.zip
    'LSMD_CONT_LDREG': 19
}
