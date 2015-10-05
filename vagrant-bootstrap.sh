#!/bin/sh

apt-get -y update
apt-get -y install build-essential
apt-get -y install curl
curl -sSL https://get.docker.com/ | sh

echo "DOCKER_OPTS='-H tcp://127.0.0.1:4243 -H unix:///var/run/docker.sock'" >> /etc/default/docker
echo "export DOCKER_HOST=tcp://localhost:4243" >> /home/vagrant/.bashrc
echo "alias dc='docker-compose'" >> /home/vagrant/.bashrc

# Install docker-compose
curl -L https://github.com/docker/compose/releases/download/1.4.2/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
restart docker

# Install NodeJS (version 4.*)
curl -sL https://deb.nodesource.com/setup_4.x | sudo -E bash -
apt-get install -y nodejs
npm install -g grunt-cli
npm install -g nodemon
