import paho.mqtt.client as mqtt

from BusAPI import *

"""
on_connect는 subscriber가 브로커에 연결하면서 호출할 함수
rc가 0이면 정상접속이 됐다는 의미
경로 설정 체크하고, 실행하기!
"""



def on_connect(client, userdata, flags, rc):
    print("connect.." + str(rc))
    if rc == 0:
        client.subscribe("eyeson/#")
    else:
        print("연결실패")


# 메시지가 도착됐을때 처리할 일들 - 여러가지 장비 제어하기, Mongodb에 저장
def on_message(client, userdata, msg):
    try:
        myval = msg.payload.decode("utf-8")
        myval = myval.replace(" ", "")
        myval = myval.split("/")
        mytopic = msg.topic.split("/")
        print(myval)
        uuid = mytopic[1]
        print(uuid)
        msg = None
        # try except에서 1보다 작은게 들어올 때 오류나는 문제 해결.
        if myval[0] == "android":
            if myval[1] == "busTime":
                target_stId = int(myval[2])
                target_busRouteId = int(myval[3])
                target_ord = int(myval[4])
                resultBusTime = arriveMessage(target_stId, target_busRouteId, target_ord)
                msgArrival = resultBusTime[3]
                indexsin = msgArrival.find("승")
                msgArrival = msgArrival[0:indexsin]
                print(msgArrival)
                pub("eyeson/" + uuid, "bigData/busTime/" + msgArrival, hostname="172.30.1.52")

            if myval[1] == "busStation":
                latitude = myval[2]  # 위도
                longitude = myval[3]  # 경도
                station = position(longitude, latitude, radius)  # 튜플
                target_stationName = station[1]
                print(target_stationName)
                print(uuid)
                pub("eyeson/" +uuid, "bigData/busStation/" + target_stationName)


            if myval[1] == "riding":
                busNum = myval[2] ## 버스번호 or 목적지명
                latitude = myval[3]  # 위도
                longitude = myval[4]  # 경도

                station = position(longitude, latitude, radius)  # 튜플

                target_stId = station[0]  # 밑 함수에서 쓰임.
                target_stationName = station[1]
                target_arsId = station[2]
                target_msgStation = station[3]
                print("정류장 ID:", target_stId)  # 정류장 ID
                print("정류장 이름:", target_stationName)  # 정류장 이름
                print("정류장 고유번호:",target_arsId)  # 정류장 고유번호
                print("사용자 메시지:", target_msgStation)  # 사용자에게 주는 메시지

                # 여기까지는 두 가지 모두 똑같음
                # ===================================================================================================
                target_bus = busNum  # str, 버스번호 or 목적지명 들어옴

                result_ordSearch = ordSearch(target_bus, target_arsId)
                target_busRouteId = result_ordSearch[0]
                target_ord = result_ordSearch[1]
                print("버스 고유번호:",target_busRouteId)
                print("해당 정류장 순번:",target_ord)

                # 에러 시 멈추고, 사용자에게 전해주는 코드 작성하기.
                if target_busRouteId == "error":
                    print("버스번호가 아님. 목적지명이거나 정류장에 없는 버스번호임")

                    busList = allBusnum(target_arsId)
                    print("출발 정류장에 들어오는 모든 버스: ", busList)

                    arrivalStation = target_bus # ex) 당산역 , or 오류나는 데이터. (잘못된 정류장 명 또는 버스 번호)
                    thebuslist = theBusnum(arrivalStation, busList)
                    print("도착 정류장에 해당하는 모든 버스: ", thebuslist)

                    if thebuslist == []:
                        publish("eyeson/" + uuid, "bigData/error/")  # 데이터 전송

                    else:
                        print("오류없이 잘 빠져나옴. 마지막 else단계")
                        waitingBusnum, waitingTime = waiting(target_arsId, thebuslist)
                        # print("후보 버스 리스트: ", waitingBusnum)
                        # print("후보 버스들의 남은 시간: ", waitingTime)

                        x = waitdeep(waitingTime)
                        destinationBus = waitingBusnum[(x.index(min(x)))]
                        destinationBusArrival = waitingTime[(x.index(min(x)))]
                        print("가장 빨리오는 버스 (최종):", destinationBus)
                        print("최종 버스 도착시간:", destinationBusArrival)

                        target_bus = destinationBus  # str, 버스번호 or 목적지명 들어옴
                        print("가장 빠른 버스 찾고 타켓버스로 지정", target_bus)

                        result_ordSearch = ordSearch(target_bus, target_arsId)
                        target_busRouteId = result_ordSearch[0]
                        target_ord = result_ordSearch[1]
                        # print("버스 고유번호:", target_busRouteId)
                        # print("해당 정류장 순번:", target_ord)

                        result_arriveMessage = arriveMessage(target_stId, target_busRouteId, target_ord)
                        arrival = result_arriveMessage[0]
                        arrival2 = result_arriveMessage[1]
                        busLicenseNum = result_arriveMessage[2]
                        msgArrival = result_arriveMessage[3]
                        busFindStatus = "bigData/ok/"

                        print("도착예정시간:", arrival)  # 첫 번째 버스 도착 예정 시간
                        print("두번째 버스 도착 예정 시간:", arrival2)  # 두 번째 버스 도착 예정 시간
                        print("차량 번호:", busLicenseNum)  # 버스 차량 번호
                        print("사용자 메시지:", msgArrival)
                        print("버스넘버 확인", destinationBus)

                        publish("eyeson/" + uuid,
                                       busFindStatus + destinationBus + "/" + msgArrival + "/" + busLicenseNum + "/" + target_stationName + "/" + str(target_stId)  + "/" + str(target_busRouteId) + "/" + str(target_ord))  # 데이터 전송
                        time.sleep(1)
                        noticeOneMinute_thread(arrival, uuid, target_stId, target_busRouteId, target_ord)


                else:
                    result_arriveMessage = arriveMessage(target_stId, target_busRouteId, target_ord)
                    # global 지움
                    arrival = result_arriveMessage[0]
                    arrival2 = result_arriveMessage[1]
                    busLicenseNum = result_arriveMessage[2]
                    msgArrival = result_arriveMessage[3]
                    busFindStatus = "bigData/ok/"

                    print("도착예정시간:",arrival)  # 첫 번째 버스 도착 예정 시간
                    print("두번째 버스 도착 예정 시간:",arrival2)  # 두 번째 버스 도착 예정 시간
                    print("차량 번호:",busLicenseNum)  # 버스 차량 번호
                    print("사용자 메시지:", msgArrival)

                    publish("eyeson/" + uuid, busFindStatus + busNum + "/" + msgArrival + "/" + busLicenseNum + "/" + target_stationName + "/" + str(target_stId)  + "/" + str(target_busRouteId) + "/" + str(target_ord))  # 데이터 전송
                    time.sleep(1)
                    noticeOneMinute_thread(arrival, uuid, target_stId, target_busRouteId, target_ord)
    except:
        pass



mqttClient = mqtt.Client()  # 클라이언트 객체 생성
# 브로커에 연결이되면 내가 정의해놓은 on_connect함수가 실행되도록 등록
mqttClient.on_connect = on_connect

# 브로커에서 메시지가 전달되면 내가 등록해 놓은 on_message함수가 실행
mqttClient.on_message = on_message

# 브로커에 연결하기
mqttClient.connect("172.30.1.52", 1883, 60)

# 토픽이 전달될때까지 수신대기
mqttClient.loop_forever()