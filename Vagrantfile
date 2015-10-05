# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.hostname = "node-mysql-wrap-production"
  config.vm.provision :shell, path: "vagrant-bootstrap.sh"
  config.vm.network :private_network, ip: "172.17.10.100"
  config.vm.synced_folder ".", "/opt/node-mysql-wrap", {:mount_options => ['dmode=777','fmode=777']}
  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.memory = 4096
    vb.cpus = 2
    vb.customize ["modifyvm", :id, "--cpuexecutioncap", "70"]
  end
end
