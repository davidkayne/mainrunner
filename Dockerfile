FROM ubuntu:22.04

ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get -y update && apt-get -y upgrade && \
    apt-get install -y software-properties-common && \
    add-apt-repository restricted && \
    apt-get install -y python3 python3-pip python3-lxml \
    tzdata curl wget pv jq aria2 ffmpeg locales neofetch \
    git make g++ gcc automake unzip mediainfo \
    autoconf libtool libcurl4-openssl-dev && \
    curl https://rclone.org/install.sh | bash && \
    # Configure time zone for Myanmar (Asia/Yangon, UTC+06:30)
    echo "Asia/Yangon" > /etc/timezone && \
    ln -sf /usr/share/zoneinfo/Asia/Yangon /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

WORKDIR /usr/src/app
RUN chmod 777 /usr/src/app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash", "start.sh"]
