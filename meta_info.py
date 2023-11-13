# ---------------------------------------------------------------------------------------------------------------------------------------------------
# 개별 테이블 정보
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# # {'ADM_SECT_C': '11110', 'SGG_NM': '\udcc1\udcbe\udcb7α\udcb8', 'SGG_OID': None, 'COL_ADM_SE': '11110'}
layer_info = {
    # 필드 순서에 맞게 정리(원본 Shape 필드 순서 확인 후 정리
    'LARD_ADM_SECT_SGG': {'layer': True,
                          'type': 'polygon',
                          'table': 'T_LNDB_L_LARD_ADM_SECT_SGG',
                          'c_table': 'T_LNDB_L_LARD_ADM_SECT_SGG_C',
                          'fields': {'ADM_SECT_CD': 'ADM_SECT_C',
                                     'SGG_NM': 'SGG_NM',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                     'SHAPE': 'geometry'},
                          'c_fields': {'ADM_SECT_CD': 'ADM_SECT_C',
                                       'SGG_NM': 'SGG_NM',
                                       'SGG_OID': 'SGG_OID',
                                       'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                       'SEQ_NO': 'SEQ_NO',
                                       'INPUT_GBN': 'INPUT_GBN',
                                       'CHG_WHERE': 'CHG_WHERE',
                                       'SHAPE': 'geometry'}
                          },
    'LSMD_ADM_SECT_UMD': {'layer': True,
                          'type': 'polygon',
                          'table': 'T_LNDB_L_LSMD_ADM_SECT_UMD',
                          'c_table': 'T_LNDB_L_LSMD_ADM_SECT_UMD_C',
                          'fields': {'EMD_CD': 'EMD_CD',
                                     'EMD_NM': 'EMD_NM',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                     'SHAPE': 'geometry'},
                          'c_fields': {'EMD_CD': 'EMD_CD',
                                       'EMD_NM': 'EMD_NM',
                                       'SGG_OID': 'SGG_OID',
                                       'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                       'SEQ_NO': 'SEQ_NO',
                                       'INPUT_GBN': 'INPUT_GBN',
                                       'CHG_WHERE': 'CHG_WHERE',
                                       'SHAPE': 'geometry'}
                          },
    'LSMD_ADM_SECT_RI': {'layer': True,
                         'type': 'polygon',
                         'table': 'T_LNDB_L_LSMD_ADM_SECT_RI',
                         'c_table': 'T_LNDB_L_LSMD_ADM_SECT_RI_C',
                         'fields': {'RI_CD': 'RI_CD',
                                    'RI_NM': 'RI_NM',
                                    'SGG_OID': 'SGG_OID',
                                    'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                    'SHAPE': 'geometry'},
                         'c_fields': {'RI_CD': 'RI_CD',
                                      'RI_NM': 'RI_NM',
                                      'SGG_OID': 'SGG_OID',
                                      'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                      'SEQ_NO': 'SEQ_NO',
                                      'INPUT_GBN': 'INPUT_GBN',
                                      'CHG_WHERE': 'CHG_WHERE',
                                      'SHAPE': 'geometry'}
                         },
    'LSMD_CONT_UI101': {'layer': True,
                        'type': 'polygon',
                        'table': 'T_LNDB_L_LSMD_CONT_UI101',
                        'c_table': 'T_LNDB_L_LSMD_CONT_UI101_C',
                        'fields': {'MNUM': 'MNUM',
                                   'ALIAS': 'ALIAS',
                                   'REMARK': 'REMARK',
                                   'NTFDATE': 'NTFDATE',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SE': 'COL_ADM_SE',
                                   'SHAPE': 'geometry'},
                        'c_fields': {'MNUM': 'MNUM',
                                     'ALIAS': 'ALIAS',
                                     'REMARK': 'REMARK',
                                     'NTFDATE': 'NTFDATE',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SE': 'COL_ADM_SE',
                                     'SEQ_NO': 'SEQ_NO',
                                     'INPUT_GBN': 'INPUT_GBN',
                                     'CHG_WHERE': 'CHG_WHERE',
                                     'SHAPE': 'geometry'}
                        },
    'LSMD_CONT_UI201': {'layer': True,
                        'type': 'polygon',
                        'table': 'T_LNDB_L_LSMD_CONT_UI201',
                        'c_table': 'T_LNDB_L_LSMD_CONT_UI201_C',
                        'fields': {'MNUM': 'MNUM',
                                   'ALIAS': 'ALIAS',
                                   'REMARK': 'REMARK',
                                   'NTFDATE': 'NTFDATE',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SEC': 'COL_ADM_SE',
                                   'SHAPE': 'geometry'},
                        'c_fields': {'MNUM': 'MNUM',
                                     'ALIAS': 'ALIAS',
                                     'REMARK': 'REMARK',
                                     'NTFDATE': 'NTFDATE',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SE': 'COL_ADM_SE',
                                     'SEQ_NO': 'SEQ_NO',
                                     'INPUT_GBN': 'INPUT_GBN',
                                     'CHG_WHERE': 'CHG_WHERE',
                                     'SHAPE': 'geometry'}
                        },
    'LSMD_CONT_LDREG': {'layer': True,
                        'type': 'polygon',
                        'table': 'T_LNDB_L_LSMD_CONT_LDREG',
                        'c_table': 'T_LNDB_L_LSMD_CONT_LDREG_C',
                        'fields': {'PNU': 'PNU',
                                   'JIBUN': 'JIBUN',
                                   'BCHK': 'BCHK',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                   'SHAPE': 'geometry'},
                        'c_fields': {'PNU': 'PNU',
                                     'JIBUN': 'JIBUN',
                                     'BCHK': 'BCHK',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                     'SEQ_NO': 'SEQ_NO',
                                     'INPUT_GBN': 'INPUT_GBN',
                                     'CHG_WHERE': 'CHG_WHERE',
                                     'SHAPE': 'geometry'}
                        }
}

# 파일 이름 안에 날짜를 가져오기위한 자리수 위치
file_date_pos = {
    # ABPM_LAND_FRST_LEDG_26_202308_Y.zip
    # ABPM_LAND_FRST_LEDG_11_20230919_Y.zip
    'ABPM_LAND_FRST_LEDG': 23,
    # APMM_NV_JIGA_MNG_11_2018_07.zip
    'APMM_NV_JIGA_MNG': 20,
    # ABPH_LAND_MOV_HIST_26_202308.zip
    # ABPH_LAND_MOV_HIST_11_20230918.zip
    'ABPH_LAND_MOV_HIST': 22,
    # ABPD_UNQ_NO_CHG_HIST_26_202308.zip
    # ABPD_UNQ_NO_CHG_HIST_46_20230621.zip
    'ABPD_UNQ_NO_CHG_HIST': 24,
    # LARD_ADM_SECT_SGG_27_202307.zip
    # LARD_ADM_SECT_SGG_43_20230825.zip
    'LARD_ADM_SECT_SGG': 21,
    # LSMD_ADM_SECT_UMD_30_202308.zip
    # LSMD_ADM_SECT_UMD_44_20230914.zip
    'LSMD_ADM_SECT_UMD': 21,
    # LSMD_ADM_SECT_RI_41_202308.zip
    # LSMD_ADM_SECT_RI_44_20230914.zip
    'LSMD_ADM_SECT_RI': 20,
    'LSCT_LAWDCD': 0,
    # LSMD_CONT_UI101_31_202308.zip
    # LSMD_CONT_UI101_41_20230725.zip
    'LSMD_CONT_UI101': 19,
    # LSMD_CONT_UI201_47_202308.zip
    # LSMD_CONT_UI201_48_20230809.zip
    'LSMD_CONT_UI201': 19,
    # LSMD_CONT_LDREG_30_202308.zip
    # LSMD_CONT_LDREG_30_20230925.zip
    'LSMD_CONT_LDREG': 19
}

real_data_nm = {'ABPM_LAND_FRST_LEDG': 'T_LNDB_L_ABPM_LAND_FRST_LEDG',
                'APMM_NV_JIGA_MNG': 'T_LNDB_L_APMM_NV_JIGA_MNG',
                'ABPH_LAND_MOV_HIST': 'T_LNDB_L_ABPH_LAND_MOV_HIST',
                'ABPD_UNQ_NO_CHG_HIST': 'T_LNDB_L_ABPD_UNQ_NO_CHG_HIST',
                'LARD_ADM_SECT_SGG': 'T_LNDB_L_LARD_ADM_SECT_SGG',
                'LSMD_ADM_SECT_UMD': 'T_LNDB_L_LSMD_ADM_SECT_UMD',
                'LSMD_ADM_SECT_RI': 'T_LNDB_L_LSMD_ADM_SECT_RI',
                'LSMD_CONT_LDREG': 'T_LNDB_L_LSMD_CONT_LDREG',
                'LSCT_LAWDCD': 'T_LNDB_L_LSCT_LAWDCD',
                'LSMD_CONT_UI101': 'T_LNDB_L_LSMD_CONT_UI101',
                'LSMD_CONT_UI201': 'T_LNDB_L_LSMD_CONT_UI201'}
