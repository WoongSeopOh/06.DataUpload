# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: NS 데이터 한달에 한번 업로드하는 프로그램
# Author: WoongSeopOh(wsoh@vng.kr)
# ---------------------------------------------------------------------------------------------------------------------------------------------------
import logging
import time
import zipfile
import arcpy
import sys
import shutil
import os
import datetime
import cx_Oracle

reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = ".AL32UTF8"

paramTest = "false"
# 1) for GP Service Publishing ----------------------------------------------------------------------------------------------------------------------
if paramTest == "true":
    arcpy.AddMessage("GP서비스 퍼블링싱을 위한 짧은 실행")
    sys.exit()

# 2) DB Connection Info -----------------------------------------------------------------------------------------------------------------------------
# 개발
connectionString = "C:/Users/Administrator/AppData/Roaming/ESRI/Desktop10.7/ArcCatalog/[DEVSVR]KGIS.sde"
dbString = "kgis/kgis@191.10.254.178:1539/test"
# 운영
# connectionString = "C:/Users/Administrator/AppData/Roaming/ESRI/Desktop10.7/ArcCatalog/[REALSVR]KGIS.sde"
# dbString = "kgis/kgis@191.10.250.200:1522/totaldb"

# 3) Setting Logger ---------------------------------------------------------------------------------------------------------------------------------
logFolder = "D:/NS_CLIENT/logs/"
logFileNm = str(datetime.datetime.now().strftime("%Y%m%d")) + "_UploadNsData.log"

UploadNsDataLogger = logging.getLogger("UploadNsDataLogger")
UploadNsDataLogger.setLevel(logging.INFO)
logFileHandler = logging.FileHandler(logFolder + logFileNm)
UploadNsDataLogger.addHandler(logFileHandler)

# 4) Decare Folder Path -----------------------------------------------------------------------------------------------------------------------------
rootPath = "D:/NS_CLIENT/Upload/"
receivePath = "D:/NS_CLIENT/RCV_FILE"

# 5) Decare Variables -------------------------------------------------------------------------------------------------------------------------------
dataList = []
sidoList = []
truncateDataList = []
zipExt = ".zip"
codePage, coordSys, cpgFile = None, None, None
brffcSDELayer = connectionString + "/KGIS.HEAT/KGIS.HPL_B_BRFFC_A"

# 6) Declare functions ------------------------------------------------------------------------------------------------------------------------------
def write_log(argmsg, argLogType):
    logtime = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : "
    UploadNsDataLogger.info(logtime + argmsg)
    if argLogType == "ERR":
        arcpy.AddError(logtime + argmsg)
    else:
        arcpy.AddMessage(logtime + argmsg)
    return


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def createJobFolder(argFolderNm):
    newNameforDelete = str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    unZip = argFolderNm + "/Unzip"
    try:
        # 1) 작업 폴더 생성
        if not os.path.exists(argFolderNm):
            os.mkdir(argFolderNm)
        else:
            newNameforDelete = newNameforDelete + "D"
            os.rename(argFolderNm, newNameforDelete)
            time.sleep(5)
            os.mkdir(argFolderNm)
            shutil.rmtree(newNameforDelete, ignore_errors=True)

        # 2) Unzip 폴더 생성
        if not os.path.exists(unZip):
            os.mkdir(unZip)
        else:
            newNameforDelete = newNameforDelete + "Z"
            os.rename(unZip, newNameforDelete)
            time.sleep(5)
            os.mkdir(unZip)
            shutil.rmtree(newNameforDelete, ignore_errors=True)
        return

    except shutil.Error, exc:
        write_log("Folder Creation Error Occurred! :" + str(exc.args[0]), "ERR")


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def moveDataUnzip(argFolder, argData):

    moveFiles = [dFile for dFile in dataFileList if dFile.startswith(data[2])]

    # 데이터별로 최종 파일 체크
    finalMoveFiles = getFilelMoveFiles(moveFiles, argData)

    for cFile in finalMoveFiles:
        # shutil.move(receivePath + "/" + mFiles, rootPath + data[4] + "/" + data[2])
        shutil.copy(receivePath + "/" + cFile, argFolder)

    # 데이터 압축해제
    dataFiles = os.listdir(argFolder)
    for zipFile in dataFiles:
        zipFileFullNm = argFolder + "/" + zipFile
        try:
            if zipFileFullNm.endswith(zipExt):
                zip_ref = zipfile.ZipFile(zipFileFullNm)
                zip_ref.extractall(unZipFolder)
                zip_ref.close()
                print (zipFile + " Unzip Finished!!")
        except zipfile.BadZipfile:
            write_log("File is not a zip file :" + str(zipFileFullNm), "ERR")
    return


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def getFilelMoveFiles(argMoveFiles, argDataNm):
    rtnList = []
    targetFiles = []
    # 데이터별로 최종 데이터를 선별한다.
    # 1) 지적 데이터 (시도별 17개가 월별로 취합되어야 한다.)
    if argDataNm == 'LSMD_CONT_LDREG':
        # 월별 데이터만 해당됨(자리수 29)
        for mFile in argMoveFiles:
            if len(mFile) == 29:
                # argMoveFiles.remove(mFile)
                targetFiles.append(mFile)

        # 동일한 이름의 데이터가 있는 경우 큰 값을 가져온다.  // sidoList 활용
        for sidoCode in sidoList:
            fileList = [nfile for nfile in targetFiles if nfile.startswith(argDataNm + '_' + sidoCode)]
            if len(fileList) > 0:
                fileList.sort(reverse=True)
                rtnList.append(fileList[0])
    else:
        rtnList = argMoveFiles

    return rtnList


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def createGDBFile(argFolder, argData):
    try:
        gdbName = argData + ".gdb"
        gdbPath = argFolder + "/" + gdbName
        if arcpy.Exists(gdbPath):
            arcpy.Delete_management(gdbPath)
            time.sleep(3)
        arcpy.CreateFileGDB_management(dataFolder, gdbName, "CURRENT")

        return gdbPath

    except arcpy.ExecuteError:
        write_log("ArcPy Error Occurred !!" + str(arcpy.GetMessages(2)), "ERR")
        # sys.exit()


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def writeLinkLog(argKey, argErrMsg, argSysNm, argDataNm):
    DBlogCon = cx_Oracle.connect(dbString)
    iCursor = DBlogCon.cursor()

    try:
        iSQL = "INSERT INTO CMT_LOG_LINK (LOG_ID, LOG_DT, CNTC_SE_NM, CNTC_SYS_NM, DATA_NM, CNTC_NRMLT_AT, CNTC_MSSAGE, PARTCLR_DC) " \
               " VALUES(SEQ_LINK_LOG_ID.NEXTVAL, SYSDATE, 'LNK003', :1, :2, :3, :4, NULL)"

        uSQL = "UPDATE CMT_LINK SET LAST_UPDT_DT = SYSDATE, LAST_STTUS_NRMLT_AT = :1, CNTC_MSSAGE = :2 WHERE SYS_CNTC_ID = " + str(argKey)

        # argErrMsg가 없으면 성공!!
        if argErrMsg is None:
            iCursor.execute(uSQL, ('Y', '연계성공'))
            iCursor.execute(iSQL, (argSysNm, argDataNm, 'Y', '연계성공'))
        else:
            iCursor.execute(uSQL, ('N', argErrMsg))
            iCursor.execute(iSQL, (argSysNm, argDataNm, 'N', argErrMsg))

        iCursor.close()
        DBlogCon.commit()
        DBlogCon.close()

    except cx_Oracle.DatabaseError as err:
        write_log("데이터베이스 오류 발생 :" + str(err), "ERR")
        iCursor.close()
        DBlogCon.close()
        sys.exit()


# functions -----------------------------------------------------------------------------------------------------------------------------------------
def setFieldMapping(argSdeDataNm, argClipDataNm):
    try:
        keyFieldName = ['OBJECTID', 'SHAPE', 'SHAPE_AREA', 'SHAPE_LENGTH', 'SHAPE.AREA', 'SHAPE.LENGTH', 'SHAPE_LEN']
        sdeFields = arcpy.ListFields(dataset=argSdeDataNm)
        clipFields = arcpy.ListFields(dataset=argClipDataNm)

        # 필드추가
        for sFld in sdeFields:
            if str(sFld.name).upper() not in keyFieldName:
                isExist = False
                for cFld in clipFields:
                    if str(sFld.name).upper() == str(cFld.name).upper():
                        isExist = True
                        break
                # DB에 있는 필드가 소스에 없으면.. 필드 추가
                if not isExist:
                    arcpy.AddField_management(in_table=argClipDataNm, field_name=sFld.name, field_type=sFld.type, field_precision=sFld.length)
                    write_log("Added Field : " + str(argClipDataNm) + ":" + str(sFld.name), "INFO")
        # 필드제거
        for cFld in clipFields:
            if str(cFld.name).upper() not in keyFieldName:
                isExist = False
                for sFld in sdeFields:
                    if str(cFld.name).upper() == str(sFld.name).upper():
                        isExist = True
                        break
                # DB에 있는 필드가 소스에 없으면.. 필드 추가
                if not isExist:
                    arcpy.DeleteField_management(in_table=argClipDataNm, drop_field=cFld.name)
                    write_log("Deleted Field : " + str(argClipDataNm) + ":" + str(cFld.name), "INFO")

        return
    except arcpy.ExecuteError:
        write_log("ArcPy Error Occurred !!" + str(arcpy.GetMessages(2)), "ERR")
        return


# 7) Service Start  ---------------------------------------------------------------------------------------------------------------------------------
write_log("NS Center Data Upload Start!!", "INFO")

# 가. 데이터 목록 추출
dbCon = cx_Oracle.connect(dbString)
dbCursor = dbCon.cursor()
try:
    # 한난맵 기본도만 업로드
    dbCursor.execute("SELECT CNTC_SYS_NM, DATA_NM, DATA_DC, UPLOAD_MTH, TGT_FOLDER, UPLOAD_DATASET, UPLOAD_TABLE, ORGINL_COORD, ORGINL_ENCODEING, CLIP_AT, SYS_CNTC_ID "
                     "  FROM CMT_LINK "
                     " WHERE DATA_DC IN('LSMD_CONT_LDREG', 'Z_KAIS_TL_SPBD_BULD', 'Z_KAIS_TL_SPBD_EQB', 'Z_KAIS_TL_SPRD_RW', 'LSMD_ADM_SECT_RI', 'LSMD_ADM_SECT_UMD', 'LARD_ADM_SECT_SGG', 'Z_KAIS_TL_SPRD_MANAGE')")
    # dbCursor.execute("SELECT CNTC_SYS_NM, DATA_NM, DATA_DC, UPLOAD_MTH, TGT_FOLDER, UPLOAD_DATASET, UPLOAD_TABLE, ORGINL_COORD, ORGINL_ENCODEING, CLIP_AT, SYS_CNTC_ID "
    #                  "  FROM CMT_LINK "
    #                  " WHERE DATA_DC IN('Z_KAIS_TL_SPRD_MANAGE')")
    for rec in dbCursor:
        dataList.append(rec)

    dbCursor.execute("SELECT SIDO_CD FROM HPL_K_LSMD_SIDO_A ")
    for rec in dbCursor:
        sidoList.append(rec[0])

    dbCursor.close()
    dbCon.close()

except cx_Oracle.DatabaseError as e:
    write_log("데이터베이스 오류 발생 :" + str(e), "ERR")
    dbCursor.close()
    dbCon.close()
    sys.exit()

# 업로드 폴더 생성여부 확인
if not os.path.exists(rootPath):
    os.mkdir(rootPath)

# 클립용 데이터 생성
try:
    brffcGdbNm = "brffcData.gdb"
    brffcGdbPath = rootPath + "/" + brffcGdbNm
    brffcGDBLayer = brffcGdbPath + "/HPL_B_BRFFC_A"
    if arcpy.Exists(brffcGdbPath):
        arcpy.Delete_management(brffcGdbPath)
        time.sleep(5)
    arcpy.CreateFileGDB_management(rootPath, brffcGdbNm, "CURRENT")

    # 지사경계 데이터 카피 (클립용)
    arcpy.CopyFeatures_management(brffcSDELayer, brffcGDBLayer, "", "0", "0", "0")

except arcpy.ExecuteError:
    write_log("ArcPy Error Occurred !!" + str(arcpy.GetMessages(2)), "ERR")
    sys.exit()

# 전체 데이터 목록 생성
dataFileList = os.listdir(receivePath)
inputDataList = ""

for data in dataList:
    # SDE 테이블 없으면 리턴 [UPLOAD_TABLE] 기준으로 체크
    # sdeDataNm = connectionString + "/KGIS." + data[5] + "/KGIS." + data[2]
    sdeDataNm = connectionString + "/KGIS." + data[5] + "/KGIS." + data[6]
    if not arcpy.Exists(sdeDataNm):
        write_log("Table is Not Exists :" + str(sdeDataNm), "INFO")
        continue

    # 시스템별로 처리
    dataFolder = rootPath + data[4] + "/" + data[2]
    unZipFolder = rootPath + data[4] + "/" + data[2] + "/Unzip"

    # 데이터별 루트 폴더 생성
    if not os.path.exists(rootPath + data[4]):
        os.mkdir(rootPath + data[4])

    # 데이터별 시스템별 작업 폴더 생성
    createJobFolder(dataFolder)
    # 데이터 이동 및 압축해제
    moveDataUnzip(dataFolder, data[2])
    # 데이터 가공(필요할 경우)
    gdbFile = createGDBFile(dataFolder, data[2])

    # 8) 좌표변환
    for uFile in os.listdir(unZipFolder):
        if uFile[-4:] == '.shp':
            # CodePage File 생성 (한글깨짐 방지용)
            cpgFile = unZipFolder + "/" + uFile.replace("shp", "cpg")
            fHandler = open(cpgFile, mode="w")
            fHandler.write(data[8])
            fHandler.close()

            try:
                # Shape파일 (Path포함)
                shpFileNm = os.path.join(unZipFolder, uFile)
                prjDataNm = gdbFile + "/" + uFile[:-4]
                clipDataNm = gdbFile + "/" + uFile[:-4] + "_Clip"
                # File GDB로 컨버전 후 업로드 (Shape이 가진 오류 해소)

                # 좌표정의 (5181 고정인 듯)
                arcpy.DefineProjection_management(shpFileNm, arcpy.SpatialReference(int(data[7])))

                # 좌표변환
                arcpy.Project_management(in_dataset=shpFileNm,
                                         out_dataset=prjDataNm,
                                         out_coor_system=arcpy.SpatialReference(5179),
                                         transform_method="",
                                         in_coor_system=arcpy.SpatialReference(int(data[7])),
                                         preserve_shape="NO_PRESERVE_SHAPE",
                                         max_deviation="",
                                         vertical="NO_VERTICAL")
                # Repair Geometry
                arcpy.RepairGeometry_management(in_features=prjDataNm,
                                                delete_null="DELETE_NULL")

                # 클립(지사경계) -- 클립인 경우만 진행
                # 인덱스인 경우만 Spatial Join
                # arcpy.SpatialJoin_analysis(target_features="D:/NS_CLIENT/Upload/NGII/Z_NGII_N3A_H0010000/Z_NGII_N3A_H0010000.gdb/Z_NGII_N3A_H0010000",
                #                            join_features="D:/NS_CLIENT/Upload/brffcData.gdb/HPL_B_BRFFC_A",
                #                            out_feature_class="D:/NS_CLIENT/Upload/NGII/Z_NGII_N3A_H0010000/Z_NGII_N3A_H0010000.gdb/Z_NGII_N3A_H0010000_SpatialJ", join_operation="JOIN_ONE_TO_ONE",
                #                            join_type="KEEP_COMMON",
                #                            field_mapping='UFID "UFID" true true false 34 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,UFID,-1,-1;DYCD "DYCD" true true false 10 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,DYCD,-1,-1;DYNM "DYNM" true true false 30 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,DYNM,-1,-1;PRDT "PRDT" true true false 6 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,PRDT,-1,-1;SCAL "SCAL" true true false 6 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,SCAL,-1,-1;MCOM "MCOM" true true false 50 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,MCOM,-1,-1;MYMD "MYMD" true true false 4 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,MYMD,-1,-1;MZON "MZON" true true false 20 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,MZON,-1,-1;PYMD "PYMD" true true false 4 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,PYMD,-1,-1;SCLS "SCLS" true true false 9 Text 0 0 ,First,#,D:\NS_CLIENT\Upload\NGII\Z_NGII_N3A_H0010000\Z_NGII_N3A_H0010000.gdb\Z_NGII_N3A_H0010000,SCLS,-1,-1',
                #                            match_option="INTERSECT", search_radius="", distance_field_name="")

                if data[9] == 'Y':
                    arcpy.Clip_analysis(in_features=prjDataNm,
                                        clip_features=brffcGDBLayer,
                                        out_feature_class=clipDataNm,
                                        cluster_tolerance="")
                else:
                    clipDataNm = prjDataNm

                # 필드 매칭 / Append를 쉽게 하기 위해 원본에 없는 필드는 추가하고, 남는 필드는 제거한다.
                setFieldMapping(sdeDataNm, clipDataNm)

                # Truncate Table list 관리 // 여러건으로 나눠진 테이블의 경우 최초 한 번 만 Truncate 되도록 수정
                if sdeDataNm not in truncateDataList:
                    arcpy.TruncateTable_management(in_table=sdeDataNm)
                    truncateDataList.append(sdeDataNm)

                # Database Upload
                arcpy.Append_management(inputs=clipDataNm,
                                        target=sdeDataNm,
                                        schema_type="TEST",
                                        field_mapping="",
                                        subtype="")

                # 건물인 경우 주소정보를 위해 전국데이터를 한 번 더 업로드 한다.
                if data[2] == 'Z_KAIS_TL_SPBD_BULD':
                    if sdeDataNm + "_FULL" not in truncateDataList:
                        arcpy.TruncateTable_management(in_table=sdeDataNm + "_FULL")
                        truncateDataList.append(sdeDataNm + "_FULL")

                    arcpy.Append_management(inputs=prjDataNm,
                                            target=sdeDataNm + "_FULL",
                                            schema_type="TEST",
                                            field_mapping="",
                                            subtype="")

            except arcpy.ExecuteError:
                write_log("ArcPy Error Occurred !!" + str(arcpy.GetMessages(2)), "ERR")
                # writeLinkLog(data[10], str(arcpy.GetMessages(2)), data[0], data[2])
                continue

            # 파일단위 후처리(예를들면, 지적도의 시도별 압축파일 처리 후)
            write_log(uFile[:-4] + " Upload Completed !!", "INFO")
            writeLinkLog(data[10], None, data[0], data[2])

    # Data 단위 후처리(예를들면, 지적도) // Index 생성 // Analyze 등


write_log("NS Center Data Upload Finished!!", "INFO")

exit()
