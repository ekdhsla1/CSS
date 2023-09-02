'''
make cropping data

v0.1 김승환 090123 - raw 데이터 크롭핑 추출 코드 완성
'''


import numpy as np
import os
import pymongo
import natsort
import shutil


############################################### 수정사항 #################################################
# 데이터 추출 선택
RAWMODE = True # raw 데이터 추출

# 1 frame에 대한 participant, maneuver 포함 여부
INITMODE = True

# MongoDB 연결 설정
client = pymongo.MongoClient("mongodb://192.168.75.251:27017/")
db = client["Test"]
collection = db["Test_SH"]
localDir = r'E:\OP_SAMPLE'
saveDir = r'E:\OP_SAMPLE'
dataType = "EXP-CN7"
participant = "FVI" # Ego,FVL,FVI,FVR,AVL,AVR,RVL,RVI,RVR
maneuver = "LK" # LK,LC
cropRange = 100 # 해당 이벤트 앞뒤에 대한 frame range
#########################################################################################################










class ExtractData():
    def __init__(self, INITMODE, localDir, saveDir, jsonDir, cropRange, jsonContent):
        print('############### EXTRACT DATA ###############\n############### CODE START ###############')

        self.localDir = localDir
        self.saveDir = saveDir
        self.jsonDir = jsonDir
        self.cropRange = cropRange
        self.jsonContent = jsonContent
        self.INITMODE = INITMODE
        self.saveDir



    def Raw(self):
        localDir = self.localDir
        saveDir = self.saveDir
        jsonDir = self.jsonDir
        cropRange = self.cropRange
        jsonContent = self.jsonContent
        INITMODE = self.INITMODE


        frameList = []
        if INITMODE:
            for initIdx in jsonContent['dynamic']['init']:
                if participant in initIdx['recognition'] and maneuver in initIdx['maneuver']:
                    frameList.append(initIdx['frameIndex'])
            for storyIdx in jsonContent['dynamic']['story']['event']:
                if participant in storyIdx['actors']['recognition'] and maneuver in storyIdx['actors']['maneuver']:
                    frameList.append(storyIdx['frameIndex'])

        if str(jsonDir).split('\\raw')[0] != str(localDir):
            rawDir = str(localDir) + '\\raw' + str(jsonDir).split('\\raw')[1]

        for folderList in os.listdir(rawDir):
            dataDir = rawDir + '\\' + folderList
            folderNum = rawDir.split('\\')[-1]
            print(f'{folderNum} DATA CROPPING...')
            save_list = self.GetCropData(cropRange, dataDir, saveDir, frameList)
        
        shutil.make_archive(save_list, 'zip', root_dir=save_list)
        shutil.rmtree(save_list)
        print(f'{folderNum} folder compression complete')

        print('############### RAW CODE END ###############')
    


    def GetCropData(self, cropRange, dataDir, saveDir, frameList):
        dataList = natsort.natsorted(os.listdir(dataDir))
        for frameIdx in natsort.natsorted(frameList):
            if frameIdx%2 == 1:
                frameIdx = frameIdx -1
            rangeMin = frameIdx - cropRange
            if rangeMin < 0:
                rangeMin = 0
            rangeMax = frameIdx + cropRange
            if 'LIDAR' in dataList[0]:
                rangeMin = int(rangeMin/2)
                rangeMax = int(rangeMax/2)+1
            if rangeMax >= len(dataList):
                rangeMax = len(dataList)-1
            saveFolder = str(saveDir) + '\\' + dataType+'_'+participant+'_'+maneuver+'_'+'cropping_raw\\' + str(dataDir).split('\\')[-2]
            if not os.path.exists(saveFolder):
                os.makedirs(saveFolder)

            cropDataList = []
            for (path, dir, files) in os.walk(dataDir):
                if 'LIDAR' in dataList[0]:
                    filteredData = natsort.natsorted(files)[rangeMin:rangeMax]
                else:
                    filteredData = natsort.natsorted(files)[rangeMin:rangeMax+1]
                for fileIdx in filteredData:
                    cropDataList.append(os.path.join(path, fileIdx))
            tmp_folder_name = str(path).split('\\')[-1]
            save_fileDir = saveFolder + '\\frame_' + str(frameIdx) + '\\' + tmp_folder_name
            if not os.path.exists(save_fileDir):
                os.makedirs(save_fileDir)
            
            for source_idx in range(len(cropDataList)):
                shutil.copyfile(cropDataList[source_idx], save_fileDir + '\\' + filteredData[source_idx])
        print(f'{tmp_folder_name} cropping is done')

        return saveFolder

        

def searchQuery(INITMODE, dataType, participant, maneuver):
    if INITMODE:
        query = {
            "$and": [
                {"dataType": dataType},
                {"dynamic.init.recognition": {"$regex": participant}},
                {"dynamic.init.maneuver": {"$regex": maneuver}},
                {"dynamic.story.event.actors.recognition": {"$regex": participant}},
                {"dynamic.story.event.actors.maneuver": {"$regex": maneuver}}
            ]
        }
    else:
        query = {
            "$and": [
                {"dataType": dataType},
                {"dynamic.story.event.actors.recognition": {"$regex": participant}},
                {"dynamic.story.event.actors.maneuver": {"$regex": maneuver}}
            ]
        }

    return query




if __name__ == "__main__":

    query = searchQuery(INITMODE, dataType, participant, maneuver)
    query_results = list(collection.find(query))
    if RAWMODE:
        print('############### RAW START ###############')
        for jsonContent in query_results:
            jsonDir = jsonContent['directory']['raw']
            
            ExtractData(INITMODE, localDir, saveDir, jsonDir, cropRange, jsonContent).Raw()
   