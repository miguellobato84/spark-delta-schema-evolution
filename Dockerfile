FROM hseeberger/scala-sbt:8u282_1.5.0_2.12.13

COPY . /home

WORKDIR /home

CMD ["sbt", "test"]