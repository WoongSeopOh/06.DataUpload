# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 유틸 모음
# ---------------------------------------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import os
import time
import shutil
import log
import zipfile
import constant

logger = log.logger


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
        exit()


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def get_folder_move_and_unzip(src_fldr, tgt_fldr, arg_data):

    if os.path.isdir(src_fldr):
        all_files = [src_file for src_file in os.listdir(src_fldr) if src_file.startswith(arg_data)]

        # 데이터별로 최종 파일 체크 (필요시)
        move_files = get_movefile_nm(all_files, arg_data)

        for m_file in move_files:
            if os.path.isfile(os.path.join(src_fldr, m_file)):
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


# isNaN ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def isNaN(num):
    return num != num


# move_and_backup_files --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def move_and_backup_files(arg_dict):
    try:
        for data in arg_dict.keys():
            if len(arg_dict[data]) > 0:
                source_folder_nm = constant.source_folder_path + data
                target_folder_nm = constant.backup_path + data
                if not os.path.isdir(target_folder_nm):
                    os.mkdir(target_folder_nm)

                for file in arg_dict[data]:
                    file_nm = source_folder_nm + '/' + file + '.zip'
                    if os.path.isfile(file_nm):
                        shutil.move(file_nm, target_folder_nm)
    except FileNotFoundError as err:
        logger.error("File Not Found Error! :" + str(err))

    return None


# validate_date ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def validate_date(arg_date):
    try:
        datetime.strptime(arg_date, "%Y%m%d")
        return True
    except ValueError:
        logger.error("Incorrect data format({0}), should be YYYYMMDD!".format(arg_date))
        return False
