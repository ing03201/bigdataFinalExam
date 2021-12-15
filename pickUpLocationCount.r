library(rhdfs)
hdfs.init()
library(rmr2)
rmr.options(backend = "hadoop")

# HDFS 상의 taxi 자료 파일 확인
hdfs.ls("/data/taxi/combined")
# 폴더에 포함된 파일 목록 files에 할당
files <- hdfs.ls("/data/taxi/combined")$file;

# info.csv에 포함된 변수 이름과 클래스 정보 읽기
mr <- mapreduce(input = files[1], 
                input.format = make.input.format(
                  format = "csv", sep=",", stringsAsFactors=F)
)
res <- from.dfs(mr) 
ress <- values(res)
colnames.tmp <- as.character(ress[,1]); colnames.tmp
class.tmp <- as.character(ress[,2]); class.tmp 
# 변수 이름
colnames <- colnames.tmp[-1]; colnames
# 변수 클래스 
class <- class.tmp[-1]; class 
class[c(6,8,9,10)] <- "numeric"

cbind(colnames, class)

# 자료의 input format 지정
input.format <- make.input.format( 
  format = "csv", sep = ",", 
  stringsAsFactors = F, 
  col.names = colnames, colClasses = class)
files <- files[-1]
files

map.fun <- function(k, v){
  # v[,13]: pickup_datetime
  # v[,15]: passenger_count
  # v[,18]: pickup_longitude
  # v[,19]: pick_latitude
  keyval(v[,13],c(v[,19],v[,18], v[,15]))
}
library(lubridate)
# payment_type을 key로 fare_amount의 합을 value로 반환
reduce.fun <- function(k, v){
  if(v[1] < 30 || v[1] > 60 || v[2] > -70 || v[2] < -90){
    latitude <- 0
    longitude <- 0
  }
  else{
    latitude <- round(v[1],digit=3)
    longitude <- round(v[2],digit=3)
  }
  dateTime <- strsplit(k, split=" ")
  date <- as.Date(dateTime[[1]][1])
  time <- strsplit(dateTime[[1]][2],split =":")
  data.frame(
    weekDay=weekdays(date),
    hour=as.numeric(time[[1]][1]),
    lat=latitude, lon=longitude, 
    peopleCount=mean(v[3])
  )
}

mr <- mapreduce( input = files[1],
                 input.format = input.format,
                 map = map.fun,
                 reduce = reduce.fun )
print(from.dfs(mr))