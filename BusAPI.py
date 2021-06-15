import threading
import requests, xmltodict
import pandas as pd
import time
from paho.mqtt import publish


# 전역변수

key = 'AT98N5LWRAir0I67tVgrf6Vfnio9LCMcwusSbOjmdkEpSOGyobdyAq9cb41G6O4pgTp6Jcmpv8e87bplMNY7tQ%3D%3D'
radius = 100  # 범위 (넓히면 여러 정류장 인식 됨.)
data1 = pd.read_csv('./data/busnumber_to_busRouteid.csv') # 경로 설정

# 빅데이터 함수
#  함수 생성 ===================================================================================================
def position(x, y, r):
    url = f'http://ws.bus.go.kr/api/rest/stationinfo/getStationByPos?ServiceKey={key}&tmX={x}&tmY={y}&radius={r}'  # 서울특별시_정류소정보조회 서비스 中 7_getStaionsByPosList
    content = requests.get(url).content
    dict = xmltodict.parse(content)

    # 첫번째 정류장이라 설정
    target_stId = int(dict['ServiceResult']['msgBody']['itemList'][0]['stationId'])
    target_stationName = str(dict['ServiceResult']['msgBody']['itemList'][0]['stationNm'])
    target_arsId = str(dict['ServiceResult']['msgBody']['itemList'][0]['arsId'])
    target_msgStation = "현재 인식된 정류장은 " + target_stationName + " 입니다."

    return (target_stId, target_stationName, target_arsId, target_msgStation)

def ordSearch(target_bus, target_arsId):
    try:
        target_busRouteId = data1[data1['busNumber'] == target_bus].iloc[0]['busRouteId']
        url = f'http://ws.bus.go.kr/api/rest/busRouteInfo/getStaionByRoute?ServiceKey={key}&busRouteId={target_busRouteId}'  # 서울특별시_노선정보조회 서비스 中 1_getStaionsByRouteList
        content = requests.get(url).content
        dict = xmltodict.parse(content)
        # target_arsId = arsId 넘버가 일치하는 버스의 seq(=ord) 구하기
        alist = []
        for i in range(0, len(dict['ServiceResult']['msgBody']['itemList'])):
            alist.append(dict['ServiceResult']['msgBody']['itemList'][i]['arsId'])
        # 인덱스 값이 곧 seq 값
        target_ord = alist.index(target_arsId) + 1
        return (target_busRouteId, target_ord)

    except:
        occurError = "error"
        errorMsg = "해당 버스번호가 존재하지 않습니다."

        return (occurError, errorMsg)

def arriveMessage(target_stId, target_busRouteId, target_ord):
    url = f'http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRoute?ServiceKey=' \
          f'{key}&stId={target_stId}&busRouteId={target_busRouteId}&ord={target_ord}'  # 서울특별시_버스도착정보조회 서비스 中 2_getArrInfoByRouteList
    content = requests.get(url).content
    dict = xmltodict.parse(content)

    arrival = dict['ServiceResult']['msgBody']['itemList']['arrmsg1']
    arrival2 = dict['ServiceResult']['msgBody']['itemList']['arrmsg2']
    busLicenseNum = dict['ServiceResult']['msgBody']['itemList']['plainNo1']
    busLicenseNum = busLicenseNum[-4:]

    if arrival == "곧 도착":
        msgArrival = "전 정류장에서 출발하여 1분 이내 도착합니다."
    else:
        index_minute = arrival.find('분')
        msgArrival = "약 " + arrival[0:index_minute+1] + " 후 도착합니다. 승차 직전 다시 알림을 주겠습니다."

    return (arrival, arrival2, busLicenseNum, msgArrival)

def noticeOneMinute(arrival, uuid, target_stId, target_busRouteId, target_ord):
    freeTime = 60  # 여유시간
    indexMinute = arrival.find('분')
    indexSecond = arrival.find('초')
    # find 이용 시, 문자열이 없으면 -1 리턴되는것을 이용
    if indexMinute == -1:
        k = 0
    elif indexSecond == -1:
        k = int(arrival[0:indexMinute]) * 60 - freeTime
    else:
        k = int(arrival[0:indexMinute]) * 60 + int(arrival[indexMinute + 1:indexSecond]) - freeTime

    waiting_stId = target_stId
    waiting_busRouteId = target_busRouteId
    waiting_ord = target_ord
    print("sleep 전")
    print(arrival, "인데", k, "초 기다려야 함")
    time.sleep(k)
    print("sleep 후")
    finalResult = arriveMessage(waiting_stId, waiting_busRouteId, waiting_ord)
    ### global finalArrival, msgFinal ###
    if finalResult[0] == "곧 도착":
        finalArrival = "잠시 후"
        time.sleep(3) # 음성인식 안겹치게 3초 추가
        msgFinal = "버스가 " + str(finalArrival) + " 도착합니다."
    else:
        finalArrival = finalResult[0]
        msgFinal = "버스가 " + str(finalArrival) + " 도착합니다."

    print("직전에 다시 예상한 도착시간:", finalArrival)
    print("직전 사용자 알림:", msgFinal)

    publish.single("eyeson/" + uuid, "bigData/last/" + msgFinal,
                   hostname="15.164.46.54")  # 데이터 전송

    return (finalArrival, msgFinal)

def noticeOneMinute_thread(arrival, uuid,  target_stId, target_busRouteId, target_ord):
    thread=threading.Thread(target=noticeOneMinute, args=(arrival, uuid,  target_stId, target_busRouteId, target_ord))
    thread.daemon = True
    thread.start()

# 목적지명 기능 관련 함수

def allBusnum(target_arsId):
    busList = []
    url = f'http://ws.bus.go.kr/api/rest/stationinfo/getStationByUid?ServiceKey={key}&arsId={target_arsId}'
    content = requests.get(url).content
    dict = xmltodict.parse(content)

    for i in range(0, len(dict['ServiceResult']['msgBody']['itemList'])):
        busList.append(dict['ServiceResult']['msgBody']['itemList'][i]['busRouteId'])

    return busList # 특정 정류장에 오는 모든 버스

def theBusnum(arrivalStation, busList):
    thebuslist = []

    for i in range(0, len(busList)):
        url = f'http://ws.bus.go.kr/api/rest/busRouteInfo/getStaionByRoute?ServiceKey={key}&busRouteId={busList[i]}'
        content = requests.get(url).content
        dict = xmltodict.parse(content)

        buslinelist = []  # 특정 버스의 노선 목록
        for i in range(0, len(dict['ServiceResult']['msgBody']['itemList'])):
            buslinelist.append(dict['ServiceResult']['msgBody']['itemList'][i]['stationNm'])

        if arrivalStation in buslinelist:
            thebuslist.append(dict['ServiceResult']['msgBody']['itemList'][i]['busRouteId'])

    return thebuslist  # arrivalStation 경유하는 버스 ID

# 비정상 상황에서도 오류가 나지않고, 빈 리스트로 리턴됨 -> 후 에러처리에 사용됨.
def searchLicenseNum(arrivalStation, busList):
    busplain = []

    for i in range(0, len(busList)):
        url = f'http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRouteAll?ServiceKey={key}&busRouteId={busList[i]}'
        content = requests.get(url).content
        dict = xmltodict.parse(content)

        buslinelists = []  # 특정 버스의 노선 목록
        for i in range(0, len(dict['ServiceResult']['msgBody']['itemList'])):
            buslinelists.append(dict['ServiceResult']['msgBody']['itemList'][i]['stNm'])

        if arrivalStation in buslinelists:
            busplain.append(dict['ServiceResult']['msgBody']['itemList'][i]['plainNo1'])

    return busplain


def waiting(target_arsId, thebuslist):
    url = f'http://ws.bus.go.kr/api/rest/stationinfo/getStationByUid?ServiceKey={key}&arsId={target_arsId}'
    content = requests.get(url).content
    dict = xmltodict.parse(content)

    waitingBusnum = []
    waitingTime = []
    for i in range(0, len(dict['ServiceResult']['msgBody']['itemList'])):
        if dict['ServiceResult']['msgBody']['itemList'][i]['busRouteId'] in thebuslist:
            x = dict['ServiceResult']['msgBody']['itemList'][i]['rtNm']
            y = dict['ServiceResult']['msgBody']['itemList'][i]['arrmsg1']
            waitingBusnum.append(x)
            waitingTime.append(y)
    return(waitingBusnum, waitingTime) # 버스 번호, 남은 시간

def waitdeep(a):
    x = []
    for i in a:
        if i == '곧 도착':
            i = 0
            x.append(i)
        elif i == '운행종료':
            i = 9999
            x.append(i)
        else:
            i = i.split('[')[0]
            index_first = i.find('분')
            index_two = i.find('초')
            i_essence = i[0:index_first]
            if index_two == -1:
                i_decimal = 0
            else:
                i_decimal = i[index_first+1:index_two]
            i = float(i_essence) * 1 + float(i_decimal) * 0.01
            x.append(float(i))
    return x