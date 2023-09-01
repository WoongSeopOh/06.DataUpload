# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 자주 사용하는 Utility를 모아 놓은 모듈
# Author: WoongSeopOh(wsoh@vng.kr)
# ---------------------------------------------------------------------------------------------------------------------------------------------------
import os


# declaration of function
# addr_to_xy-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Description:
# Parameter:
# Return Value:
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def search_files(arg_dir, arg_ext_nm):
    rtn_list = []
    file_list = os.listdir(arg_dir)

    for file_nm in file_list:
        full_nm = os.path.join(arg_dir, file_nm)
        if os.path.isfile(full_nm):
            ext_nm = os.path.splitext(full_nm)[-1]
            if str(ext_nm).lower() == arg_ext_nm:
                # print(os.path.join(argDir, file_nm))
                rtn_list.append(os.path.join(arg_dir, file_nm))
    return rtn_list
