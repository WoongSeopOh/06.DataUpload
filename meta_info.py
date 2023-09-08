# ---------------------------------------------------------------------------------------------------------------------------------------------------
# 개별 테이블 정보
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# # {'ADM_SECT_C': '11110', 'SGG_NM': '\udcc1\udcbe\udcb7α\udcb8', 'SGG_OID': None, 'COL_ADM_SE': '11110'}
layer_info = {
    # 필드 순서에 맞게 정리(원본 Shape 필드 순서 확인 후 정리
    # todo: 원본 테이블에서 OBJECTID 제외 필요
    'LARD_ADM_SECT_SGG': {'layer': True,
                          'type': 'polygon',
                          'fields': {'ADM_SECT_CD': 'ADM_SECT_C',
                                     'SGG_NM': 'SGG_NM',
                                     'SGG_OID': 'SGG_OID',
                                     'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                     'OBJECTID': None,
                                     'SHAPE': 'geometry'}},
    'LSMD_CONT_LDREG': {'layer': True,
                        'type': 'polygon',
                        'fields': {'PNU': 'PNU',
                                   'JIBUN': 'JIBUN',
                                   'BCHK': 'BCHK',
                                   'SGG_OID': 'SGG_OID',
                                   'COL_ADM_SECT_CD': 'COL_ADM_SE',
                                   'SHAPE': 'geometry'}}
}

layer_nm = {
    'LARD_ADM_SECT_SGG': 'T_LNDB_L_LARD_ADM_SECT_SGG',
    'LSMD_CONT_LDREG': 'T_LNDB_L_LSMD_CONT_LDREG'
}
