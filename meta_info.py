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

layer_nm = {
    'LARD_ADM_SECT_SGG': '',
    'LSMD_CONT_LDREG': ''
}

# # 전체 데이터 변경(7): 시군구, 읍면동, 리, 용도구역, 접도구역, 법정동코드, 공시지가
# all_data_nm = {
#     'LARD_ADM_SECT_SGG': 'T_LNDB_L_LARD_ADM_SECT_SGG',
#     'LSMD_ADM_SECT_UMD': '',
#     'LSMD_ADM_SECT_RI': '',
#     'LSMD_CONT_UI101': '',
#     'LSMD_CONT_UI201': '',
#     'LSCT_LAWDCD': 'T_LNDB_L_LSCT_LAWDCD',
#     'APMM_NV_JIGA_MNG': 'T_LNDB_L_APMM_NV_JIGA_MNG'
# }
